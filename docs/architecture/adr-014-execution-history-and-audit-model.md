# ADR-014 — Execution History & Audit Model

## 1. Purpose

This Architectural Decision Record (ADR) establishes the execution history and audit architecture for the NexusFlow orchestration engine. It defines how the engine durably records what occurred, when it occurred, across which entities, why it occurred, and by whom, without converting the audit subsystem into an authoritative state store, an event-sourced recovery mechanism, a secondary state machine, or a scheduling correctness dependency.

Furthermore, this record formalizes the structural boundary between authoritative current state and explanatory execution history, the atomic durability relationship between state transitions and history entries, the one-record-per-semantic-commit model, data minimization principles, and ordering semantics, while deferring physical storage implementations to [ADR-020](00-architecture-decision-register.md) and telemetry concerns to [ADR-016](00-architecture-decision-register.md).

---

## 2. Context

NexusFlow executes multi-step DAG workflows whose runtime behavior is governed by formal state machines defined in [ADR-006](adr-006-workflow-execution-state-machine.md) (`WorkflowExecution`) and [ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md) (`TaskExecution` and `ExecutionAttempt`). Distributed worker coordination is established in [ADR-008](adr-008-worker-coordination-and-liveness-model.md), task routing in [ADR-009](adr-009-task-routing-strategy.md), and parameter passing in [ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md).

Authoritative orchestration durability is governed by [ADR-011](adr-011-state-persistence-strategy.md), which established a normalized, durable current-state persistence model across ten logical consistency groups. [ADR-012](adr-012-recovery-strategy.md) established the post-crash reconciliation strategy based on authoritative current state without event replay or global recovery locks. [ADR-013](adr-013-consistency-and-concurrency-strategy.md) established durable optimistic single-winner concurrency control, short atomic persistence transactions, and independent parallel task progression.

### The Operational Need for Execution History
While authoritative current state answers *where the system is right now*, operators, developers, audit systems, and user interfaces require answers to historical and diagnostic questions:
1. What lifecycle transitions and causal progression led to the current state?
2. Why did a specific task retry, and what was the failure cause of Attempt 1?
3. Which worker session claimed Attempt 2, and when was execution start observed?
4. Did a worker execution result win the race against an execution timeout, or vice versa?
5. When was a workflow cancellation accepted, and how did parallel branches drain?
6. What specific state repairs were performed by crash recovery after an orchestrator restart?

NexusFlow requires an audit and history model that provides trustworthy answers to these questions while preserving the core persistence, recovery, and concurrency invariants established in ADR-011, ADR-012, and ADR-013.

---

## 3. Problem Statement

How should NexusFlow record execution history and audit information so that users and operators can reconstruct and understand past execution behavior without:
1. Transforming the audit/history subsystem into the authoritative execution state store?
2. Introducing event-sourcing replay dependencies into the scheduler or crash recovery?
3. Creating a secondary state machine that can desynchronize from authoritative current state?
4. Introducing a hot workflow-level sequence counter that serializes independent parallel DAG tasks and violates ADR-013?
5. Permitting state transitions to commit while their corresponding audit records are lost?
6. Bloating the persistence store by duplicating large input and output payloads into historical records?
7. Inundating the audit log with high-frequency operational noise (e.g., worker heartbeats, routing candidate offers, lost OCC races)?
8. Causing crash recovery or scheduling correctness to depend on history retention or history database availability?

---

## 4. Requirements Covered

### Functional History Requirements
- **FR-HIST-001:** Durably record all authoritative lifecycle mutations across workflows, tasks, attempts, retries, cancellations, and state-altering recovery repairs.
- **FR-HIST-002:** Capture semantic causal context (actor, source, previous state, new state, failure cause, attempt ordinal, worker session) in each history record.
- **FR-HIST-003:** Guarantee atomic consistency between an auditable state transition and its corresponding history entry.
- **FR-HIST-004:** Prevent duplicate history generation across duplicate callbacks, duplicate timers, and retried unknown-commit reconciliations.
- **FR-HIST-005:** Provide bounded, paginated query capabilities across workflow, task, attempt, and temporal dimensions.

### Non-Functional History Requirements
- **NFR-COR-002 (Authority Isolation):** Authoritative current state must remain the sole source of truth for scheduling, recovery, routing, and concurrency control. Schedulers and recovery sweeps must never read or replay history.
- **NFR-SCA-002 (Concurrency Preservation):** History generation must not require a global or workflow-wide monotonic sequence counter, preserving independent task progression under ADR-013.
- **NFR-DAT-001 (Payload Minimization):** History records must not duplicate workflow or task input/output payloads, referencing authoritative storage instead.
- **NFR-REL-003 (Retention Decoupling):** Historical record archiving or purging must have zero effect on active or terminal execution correctness under ADR-011/012.

---

## 5. Constraints

1. **Current State Supremacy:** History must never supersede or define authoritative current state (preserving ADR-011).
2. **Zero Recovery Dependence:** Crash recovery must never replay history records to reconstruct state (preserving ADR-012).
3. **OCC Alignment:** History creation must participate in the short atomic persistence transactions of ADR-013 without holding transactions open across network I/O.
4. **Technology Neutrality:** ADR-014 must not specify SQL tables, database engines, sequence generators, or indexing syntaxes (owned by [ADR-020](00-architecture-decision-register.md)).
5. **Telemetry Decoupling:** ADR-014 must not encompass operational metrics, distributed traces, or raw debug logs (owned by [ADR-016](00-architecture-decision-register.md)).
6. **Security & IAM Decoupling:** ADR-014 captures abstract initiator references but does not define authentication, RBAC, or compliance frameworks (owned by [ADR-022](00-architecture-decision-register.md)).

---

## 6. Goals

- Define an append-oriented, immutable execution history model capturing all authoritative orchestration transitions.
- Enforce write-path atomicity between state mutations and their corresponding history records.
- Establish the one-record-per-semantic-commit model across multi-entity consistency groups.
- Exclude ephemeral operational noise (heartbeats, routing candidates, lost races, no-ops) from durable history.
- Establish a causal and presentation ordering model that avoids central workflow sequence bottlenecks.
- Enforce payload minimization and sensitive data protection across all history records.
- Decouple history retention and purging from engine correctness.

---

## 7. Non-Goals

- Implementing an event-sourced architecture where current state is derived by event replay.
- Defining physical database schemas, column types, or storage engines (owned by [ADR-020](00-architecture-decision-register.md)).
- Defining REST API routes, pagination token formats, or HTTP schemas (owned by [ADR-015](00-architecture-decision-register.md)).
- Implementing application performance metrics, OpenTelemetry spans, or log aggregation (owned by [ADR-016](00-architecture-decision-register.md)).
- Defining formal error taxonomies or string error codes (owned by [ADR-018](00-architecture-decision-register.md)).
- Defining cryptographic tamper-evident ledgers, blockchains, or Merkle hash chains for compliance.
- Duplicating full business input/output payloads in history records.

---

## 8. Candidate Solutions

### Candidate A: Telemetry and Logging Only (No Durable History)
Rely entirely on application logs, distributed traces, and metrics (ADR-016) to capture execution timelines.
- *Pros:* Zero persistence overhead; simple engine implementation.
- *Cons:* Logs are subject to loss, truncation, buffering drops, and aggressive retention rotations; fails to provide audit-quality durability; cannot provide reliable timeline queries via user API.

### Candidate B: Event Sourcing as Source of Truth
Replace the normalized current-state persistence model with an append-only event log. Current state is reconstructed by replaying events from the beginning of time or periodic snapshots.
- *Pros:* Natural unified audit log; state and history are physically identical.
- *Cons:* Directly contradicts ADR-011 and ADR-012; forces complex event versioning and migration logic; requires snapshotting; introduces avoidable concurrency bottlenecks on workflow event streams; significantly complicates recovery.

### Candidate C: Best-Effort Asynchronous Audit Log
Current-state mutations commit normally. An asynchronous background task or message listener captures the transition and appends a history record out-of-band.
- *Pros:* Minimal latency overhead on state transactions; failure to write history never blocks execution.
- *Cons:* An orchestrator crash between the state commit and the history write permanently loses the audit record; breaks audit integrity; produces torn or missing timelines.

### Candidate D (Selected): Atomic Append-Oriented Execution History
Normalized current state remains the authoritative truth. For every required auditable state mutation, the current-state mutation and exactly one corresponding `HistoryEntry` commit within the **same durable atomic persistence transaction**. History is append-only and immutable during retention lifetime. Schedulers and recovery never read history.
- *Pros:* Establishes that no committed state change exists without its audit record; avoids event-sourcing replay complexity; recovery remains independent; preserves parallel DAG task progression under ADR-013.
- *Cons:* Increases the write footprint of state mutation transactions; requires fail-closed handling if history persistence is unavailable.

---

## 9. Detailed Evaluation

| Evaluation Criterion | Candidate A (Logs Only) | Candidate B (Event Sourcing) | Candidate C (Async Best-Effort) | Candidate D (Atomic Append - Selected) |
| :--- | :--- | :--- | :--- | :--- |
| **State Authority** | Current State | Event Log Replay | Current State | **Authoritative Current State** |
| **Audit Reliability** | Poor (rotates/drops) | Inherent (is state) | Fragile (crashes drop records) | **High (atomic with state commit)** |
| **Recovery Independence**| High | Zero (depends on replay) | High | **Strict (zero history dependency)** |
| **Parallel Scalability** | High | Low (stream serialization)| High | **High (tied to entity-level OCC)** |
| **Operational Simplicity**| High | Low (event migration/snapshots) | Moderate | **High (normalized state + append log)** |
| **Replay Dependence** | None | High | None | **None (replay explicitly prohibited)** |
| **Crash Consistency** | Inconsistent | Consistent | Inconsistent | **Consistent (atomic commit boundary)** |

---

## 10. Decision

NexusFlow adopts **Atomic Append-Oriented Execution History** as its execution history and audit architecture.

### 10.1 Core Architectural Invariants
1. **Current State Supremacy:** Authoritative durable current state (`WorkflowExecution`, `TaskExecution`, `ExecutionAttempt`) is the sole truth governing scheduling, routing, dispatch, retries, dependency satisfaction, active attempt authority, and recovery.
2. **Zero History Replay:** Schedulers, workers, and recovery reconciliation sweeps must **never** read, replay, or depend on `HistoryEntry` records to determine execution state. Replay is permitted only for human/UI visualization and offline debugging.
3. **Write-Path Durability Atomicity:** For every required auditable authoritative orchestration mutation, the current-state mutation and exactly one corresponding `HistoryEntry` must commit within the **same durable atomic persistence transaction**.
4. **History Write Failure Fails Closed:** If the required `HistoryEntry` cannot be persisted, the associated authoritative state transition must not commit.
5. **History Read Failure Isolation:** A failure in the history query or retrieval path affects only operators and UI viewers; it must never block or fail active workflow scheduling or crash recovery.
6. **One Record per Semantic Commit:** Exactly one `HistoryEntry` is emitted per consistency-group commit, capturing the multi-entity transition atomically rather than emitting fragmented per-entity records.
7. **Append-Only Immutability:** Once committed, a `HistoryEntry` is permanently immutable during its retention lifetime. In-place updates are prohibited.
8. **Payload Minimization:** History records must never duplicate workflow or task input/output payloads. Authoritative payloads reside strictly in current-state entities.
9. **Descriptive Timestamp Semantics:** History records store a control-plane recorded/committed timestamp (`committed_at`) associated with the durable commit. Control-plane recorded timestamps are the trusted timestamp source for audit metadata, but timestamps themselves are not the semantic ordering authority.
10. **Untrusted Worker Clocks:** Worker wall-clock timestamps are untrusted diagnostic metadata and must never govern history ordering, timeout evaluations, or state transitions.
11. **No Hot Workflow History Sequence:** No global or workflow-wide monotonic sequence counter is maintained, preserving the independent task concurrency established in ADR-013.
12. **Retention Decoupling:** Archiving or purging historical records after terminal execution does not affect scheduling/recovery correctness under ADR-011/012. Active execution history should be retained until terminalization for auditability.

### 10.2 Semantic Model of `ExecutionHistoryEntry`
A `HistoryEntry` conceptually contains the following technology-neutral fields:
- **`history_entry_id`:** Stable, operationally unique identifier (representation owned by ADR-020).
- **`workflow_execution_id`:** Correlation to parent workflow.
- **`task_execution_id` (optional):** Present for task-level operations.
- **`attempt_id` (optional):** Present for attempt-level operations.
- **`attempt_ordinal` (optional):** Logical ordinal if an attempt is involved.
- **`event_category`:** Semantic operation category.
- **`actor` / `source`:** Abstract origin of the transition (`API_USER`, `SCHEDULER`, `WORKER`, `TIMER`, `RECOVERY`, `SYSTEM`).
- **`from_state` / `to_state` (optional):** State transition facts for primary participating entities.
- **`committed_at`:** Authoritative control-plane recorded/committed timestamp associated with the durable history commit.
- **`cause` / `diagnostic_metadata` (optional):** Structured, bounded metadata explaining why the transition occurred.

### 10.3 Required History-Worthy Transitions
Required durable history encompasses:
1. **Workflow Lifecycle Transitions:**
   - `WorkflowExecution` creation in `INITIALIZING`.
   - `INITIALIZING -> RUNNING` (Authoritative execution start).
   - `INITIALIZING -> CANCELLING` (Cancellation during initialization).
   - `INITIALIZING -> FAILED` (Definitive semantic initialization failure under ADR-006).
   - `RUNNING -> FAILING` (Workflow failure direction commit).
   - `RUNNING -> CANCELLING` (Workflow cancellation direction commit).
   - `RUNNING -> SUCCEEDED` (Workflow completion & output commit).
   - `FAILING -> FAILED` (Workflow terminal failure drain completion).
   - `CANCELLING -> CANCELLED` (Workflow terminal cancellation drain completion).
2. **Initial Task-Set Establishment:** The workflow initialization semantic history record summarizes establishment of the expected `TaskExecution` set, avoiding $N$ redundant entries.
3. **Task & Attempt Lifecycle Consistency Groups:**
   - Group 1: Task `PENDING -> RUNNABLE` + input materialization fact.
   - Group 2 (Ownership Commit): Task `RUNNABLE -> RUNNING` + `ExecutionAttempt` created in `CLAIMED` + attempt ordinal + `WorkerSessionId`.
   - Group 3: `ExecutionAttempt` `CLAIMED -> RUNNING`.
   - Group 4: `ExecutionAttempt` `RUNNING -> SUCCEEDED` + `TaskExecution` `RUNNING -> SUCCEEDED` + authoritative task output committed.
   - Group 5: `ExecutionAttempt` `RUNNING -> FAILED` + `TaskExecution` `RUNNING -> RETRY_WAIT` + retry backoff timing.
   - Group 6: `ExecutionAttempt` `RUNNING -> FAILED` + `TaskExecution` `RUNNING -> FAILED` (no retry).
   - Unstarted Task Cancellation: Task in `PENDING / RUNNABLE / RETRY_WAIT -> CANCELLED`.
   - Active Attempt Cancellation Settlement: `ExecutionAttempt -> CANCELLED` + `TaskExecution -> CANCELLED`.
4. **Authoritative Recovery Actions:** State-altering recovery repairs under ADR-012 (e.g., partial-initialization repair, overdue start timeout enforcement, workflow direction repair, invariant quarantine).

### 10.4 Conceptual Semantic Event Categories
The following illustrative categories describe the semantic operations (exact serialized identifiers belong to ADR-020/LLD; formal error/cause taxonomies remain ADR-018):
- **Workflow:** `WORKFLOW_INITIALIZED`, `WORKFLOW_STARTED`, `WORKFLOW_FAILING`, `WORKFLOW_CANCELLING`, `WORKFLOW_SUCCEEDED`, `WORKFLOW_FAILED`, `WORKFLOW_CANCELLED`.
- **Task & Attempt:** `TASK_RUNNABLE`, `ATTEMPT_OWNERSHIP_COMMITTED`, `ATTEMPT_STARTED`, `TASK_ATTEMPT_SUCCEEDED`, `TASK_RETRY_SCHEDULED`, `TASK_FAILED`, `TASK_CANCELLED`, `ATTEMPT_CANCELLED`.
- **Recovery & Integrity:** `RECOVERY_REPAIR_PERFORMED`, `EXECUTION_QUARANTINED`.

### 10.5 Non-History Operational Activity (Explicitly Excluded)
The following activities must **never** generate durable execution history entries (they belong to ADR-016 telemetry or ephemeral memory):
- Pre-ownership routing candidate selection, worker offers, and offer rejections.
- Periodic worker session heartbeats and heartbeat renewals.
- Read-only API queries (status polling, workflow listing).
- Scheduler evaluation passes that find no ready work or result in no state mutation.
- Lost optimistic concurrency (OCC) races and transient transaction retries.
- Duplicate worker callbacks or duplicate timer firings that resolve idempotently as no-ops.
- Routine background recovery scans that find all states valid.
- Raw worker stdout/stderr application logs.

---

## 11. Decision Rationale

### 11.1 Why Not Event Sourcing?
Event-sourced engines derive current state by replaying an append-only event log. While conceptually unified, event sourcing introduces significant operational and architectural burdens:
- Replaying events to reconstruct DAG state during crash recovery introduces variable, potentially unbounded restart latency.
- Schema evolution requires complex event upcasting or long-term multi-version deserializers.
- Enforcing cross-entity invariants (such as active attempt uniqueness or dependency satisfaction) across parallel branches requires global event stream locks or complex projection read-models.

NexusFlow's normalized durable current-state model ([ADR-011](adr-011-state-persistence-strategy.md)) provides direct current-state reads without event replay. Pairing this with atomic append-oriented history provides durable execution auditability for required recorded mutations without sacrificing recovery speed or operational simplicity.

### 11.2 Why Atomic State + History Commit?
Asynchronous or best-effort history logging introduces an unresolvable vulnerability: if the orchestrator process crashes after committing a state transition but before writing the audit log, the audit timeline is permanently corrupted. By including the `HistoryEntry` write within the existing ADR-011 consistency-group transaction, NexusFlow establishes that a state change cannot exist without its audit record, and no history entry can exist for a mutation that rolled back.

### 11.3 Why One Record per Semantic Commit?
In consistency groups spanning multiple entities (e.g., Group 4 settling an attempt, settling a task, and committing output; or Group 2 committing task ownership and creating an attempt), emitting separate history entries for each entity creates timeline fragmentation and risks observing torn events. A single `HistoryEntry` captures the complete, atomic semantic operation, preserving causal relationships and reducing write overhead.

### 11.4 History Ordering Without a Hot Sequence Counter
A common architectural trap is enforcing a monotonic integer counter (`WorkflowExecution.history_sequence`) across all history entries in a workflow. In a widely branching DAG, every parallel task completion would be forced to lock the root workflow row solely to increment the history counter. This would directly destroy the independent task concurrency established in [ADR-013](adr-013-consistency-and-concurrency-strategy.md).

NexusFlow rejects global and workflow-wide monotonic sequence counters. Authoritative ordering is established through:
1. **Entity Causal Ordering:** For events on the **same entity**, ordering is established by legal state machine progression, entity revision advancement, and attempt ordinals.
2. **Descriptive Control-Plane Timestamps:** Every `HistoryEntry` records `committed_at`, the control-plane timestamp associated with the durable persistence commit.
3. **Presentation Disambiguation:** For independent parallel tasks that commit with identical or near-identical timestamps, presentation layers (e.g., UI timelines) apply a deterministic tie-breaker (e.g., storage transaction sequence, unique entry identity, or stable correlation keys). Physical tie-breaker mechanics belong to [ADR-020](00-architecture-decision-register.md).

---

## 12. Tradeoffs

| Advantage | Tradeoff / Mitigation |
| :--- | :--- |
| **Audit Completeness:** State transitions and audit records never desynchronize. | **Transaction Footprint:** Adds an insert operation to each state persistence transaction. *Mitigation:* History entries are small, structured, and contain no payloads. |
| **Fast Recovery:** Recovery reads indexed current state directly without event replay. | **Dual Storage Responsibility:** Persistence layer stores both current state and historical stream. *Mitigation:* Clean separation of entities; history can be archived independently. |
| **Parallel DAG Scalability:** Eliminates workflow-level sequence locks. | **Presentation Ordering:** Requires deterministic sorting on `committed_at` and storage tie-breakers for UI timelines. *Mitigation:* Standard query pagination with stable secondary keys. |
| **Data Safety & Lean Storage:** Payload minimization keeps history lean and safe. | **Indirect Payload Inspection:** Viewing outputs requires cross-referencing task current-state records. *Mitigation:* API layer provides unified views. |

---

## 13. Consequences

### 13.1 Positive Consequences
- **Complete Timeline Auditability:** Every lifecycle change, ownership claim, retry, cancellation, and recovery action is permanently recorded.
- **Strict Invariant Defense:** Schedulers and recovery cannot be corrupted by history bugs because they never read history.
- **Idempotent Reconciliation:** Post-disconnect unknown-commit re-reads detect whether a transition and its history already committed, preventing duplicate entries.
- **Controlled Storage Growth:** Because business payloads are excluded and high-frequency operational events (heartbeats, routing offers) are omitted, history growth is driven primarily by recorded semantic lifecycle mutations rather than high-frequency operational events.
- **Clean Retention Model:** Terminal executions can have their history archived or purged without impacting running workflows or recovery routines under ADR-011/012.

### 13.2 Negative / Constraining Consequences
- **Write-Path Dependency:** A failure in the history storage subsystem blocks authoritative state transitions (fail closed).
- **No In-Place Corrections:** Diagnostic errors in committed history entries cannot be corrected via in-place mutation; corrective annotations must be appended if needed.
- **Storage Demands on ADR-020:** The physical storage engine must support atomic multi-entity inserts alongside state updates.

---

## 14. Failure Modes & Concurrency Arbitrations

| # | Scenario / Failure Mode | Current-State Fact | Event / Trigger | Authoritative State Transition | History Behavior | Orchestration Impact | Owning ADR |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | **History Write Fails** | Transition uncommitted | Storage write fails | Transaction aborted; rolls back | History write fails; rolled back with state | Transition does not commit; safe retry | ADR-014 / ADR-011 |
| **2** | **State Write Fails** | Transition uncommitted | Storage constraint fails | Transaction aborted; rolls back | Zero history entries written | Transition does not commit; error bubbled | ADR-014 / ADR-011 |
| **3** | **Crash Before Commit** | Uncommitted | Process crash | Storage engine rolls back uncommitted transaction | Zero history entries written | Post-restart recovery handles state | ADR-014 / ADR-012 |
| **4** | **Crash Immediately After Commit** | Committed | Process crash | Durable truth intact | History entry is safely committed | Restart recovery reconciles authoritative committed current state. The associated HistoryEntry already remains durably persisted but is not required by recovery | ADR-014 / ADR-012 |
| **5** | **Unknown Commit Result** | Unknown (network drop) | Transport disconnect | Re-read authoritative state by stable identity | Inspect if state + history exist | Idempotent resolution; no duplicate history | ADR-014 / ADR-013 |
| **6** | **Duplicate Result Callback** | Task/Attempt `SUCCEEDED` | Worker resends result | Detect idempotent match | Zero new history entries appended; no-op | No state or history duplication | ADR-014 / ADR-010 |
| **7** | **Duplicate Timeout Callback** | Attempt terminal | Timeout timer fires again | Matches 0 rows on active state; no-op | Zero new history entries appended; no-op | No state or history duplication | ADR-014 / ADR-007 |
| **8** | **Lost OCC Race** | Stale attempt | Competing write wins | Loser rolls back | Zero history entries written for lost attempt | Loser re-reads and resolves | ADR-014 / ADR-013 |
| **9** | **Group 4 Success + Output** | Task/Attempt `RUNNING` | Result arrives | Attempt `SUCCEEDED`, Task `SUCCEEDED`, output committed | Emits single semantic history entry | Authoritative completion recorded | ADR-014 / ADR-010 |
| **10**| **Group 5 Retry Scheduling** | Task/Attempt `RUNNING` | Attempt fails, budget $>0$ | Attempt `FAILED`, Task `RETRY_WAIT`, timer persisted | Emits single semantic history entry | Retry scheduled with backoff timing | ADR-014 / ADR-007 |
| **11**| **Group 6 Definitive Failure** | Task/Attempt `RUNNING` | Attempt fails, no retry | Attempt `FAILED`, Task `FAILED` | Emits single semantic history entry | Task definitive failure recorded | ADR-014 / ADR-007 |
| **12**| **Ownership Commit (Group 2)** | Task `RUNNABLE` | Worker claims task | Task `RUNNING`, Attempt `CLAIMED`, ordinal assigned | Emits single semantic history entry | Ownership and attempt creation recorded | ADR-014 / ADR-008 |
| **13**| **Workflow Cancellation** | Workflow `RUNNING` | User cancel request | Workflow `RUNNING -> CANCELLING` | Emits workflow cancellation history entry | Drain direction established | ADR-014 / ADR-006 |
| **14**| **Workflow Success (Group 10)** | All tasks `SUCCEEDED` | Final task completes | Workflow `RUNNING -> SUCCEEDED`, output committed | Emits workflow success history entry | Workflow completion recorded | ADR-014 / ADR-006 |
| **15**| **Workflow Failure Direction** | Task `FAILED` | Failure direction commit | Workflow `RUNNING -> FAILING` | Emits workflow failure direction entry | Drain direction established | ADR-014 / ADR-006 |
| **16**| **Workflow Terminalization** | Workflow `FAILING` / `CANCELLING`| All tasks terminal | Workflow `-> FAILED` or `-> CANCELLED` | Emits terminal completion history entry | Terminal lifecycle recorded | ADR-014 / ADR-006 |
| **17**| **Initial Task-Set Creation** | Workflow `INITIALIZING` | Workflow creation | Initial expected `TaskExecution` set created | Initialization entry summarizes task set creation | Single history entry for workflow init | ADR-014 / ADR-006 |
| **18**| **Recovery Repairs Task Set** | Missing `TaskExecution` | Recovery reconciliation | Missing `TaskExecution` records inserted | Emits recovery repair audit entry | Recovery action documented | ADR-014 / ADR-012 |
| **19**| **Clean Recovery Scan** | Valid state | Scheduled scan | No state change | Zero history entries emitted (no noise) | No storage bloat | ADR-014 / ADR-012 |
| **20**| **Worker Loss Determination** | Active Attempt | Heartbeat grace expires | Attempt `FAILED` (worker-loss cause); Task settles | Emits single semantic history entry with cause | Failure recorded with worker-loss cause | ADR-014 / ADR-008 |
| **21**| **Pre-Ownership Routing Failure**| Task `RUNNABLE` | Worker rejects offer | Task remains `RUNNABLE` | Zero history entries emitted (ephemeral) | Telemetry only (ADR-016) | ADR-014 / ADR-009 |
| **22**| **Worker Heartbeat** | Active Attempt | Heartbeat received | Memory registry updated | Zero history entries emitted (ephemeral) | Liveness only; zero history bloat | ADR-014 / ADR-008 |
| **23**| **Stale Attempt Result** | Superseded Attempt | Delayed result arrives | Fails active attempt predicate; rejected | Zero history entries emitted | Security/debug telemetry only | ADR-014 / ADR-007 |
| **24**| **Parallel Task Transitions** | Independent branches | Tasks complete concurrently | Independent commits on separate tasks | Each emits independent history entry | Concurrent branch progression | ADR-014 / ADR-013 |
| **25**| **Same-Entity Rapid Transitions**| Same Task/Attempt | Rapid lifecycle events | Serialized transitions advance revision/state | Ordered by revision/ordinal in history | Causally ordered execution timeline | ADR-014 / ADR-013 |
| **26**| **Worker Clock Skew** | Active Attempt | Worker reports bad time | State committed normally | `committed_at` uses orchestrator clock | Untrusted worker clock ignored | ADR-014 / ADR-008 |
| **27**| **Equal Commit Timestamps** | Independent tasks | Simultaneous commits | Both commit independently | Disambiguated by deterministic tie-breaker | Presentation order stable | ADR-014 / ADR-020 |
| **28**| **History Read API Unavailable** | State valid | User queries history | Read error returned to client | History query fails; write path unaffected | Engine continues scheduling safely | ADR-014 / ADR-015 |
| **29**| **History Storage Unavailable** | State transition attempted | Storage disconnect | Transaction fails closed and rolls back | Write fails | Engine halts state mutation safely | ADR-014 / ADR-011 |
| **30**| **History Corruption Detected** | State valid | Integrity scan | Never replayed to mutate current state | Report/isolate the detected integrity fault according to ADR-018/ADR-020 policy | Fail-safe: investigate fault; state unaffected | ADR-014 / ADR-018 |
| **31**| **History Purge** | Terminal workflow | Retention job runs | Current state remains intact | Historical records deleted/archived | Does not affect scheduling/recovery correctness under ADR-011/012 | ADR-014 |
| **32**| **Active-Execution Purge Attempt**| Active workflow | Misconfigured retention | Blocked by retention policy | Active history preserved | Audit timeline protected | ADR-014 |
| **33**| **Recovery After History Purge** | Terminal workflow | Orchestrator restart | Current state inspected directly | History absent | Recovery succeeds without history | ADR-014 / ADR-012 |
| **34**| **Attempted Replay of History** | Any state | Tool/operator invocation | Engine rejects replay as state mutation | History read-only | Engine state protected | ADR-014 |
| **35**| **Duplicate History Entry** | Any state | Retried transaction | Prevented by atomic state-history commit | Exactly one entry per semantic commit | Duplicate prevention guaranteed | ADR-014 / ADR-013 |
| **36**| **Conflicting Metadata on Retry** | Any state | Worker retry mismatch | Output immutability rejects conflict | Existing history remains authoritative | Diagnostic anomaly logged | ADR-014 / ADR-010 |

---

## 15. Debugging

`ExecutionHistoryEntry` serves as the primary post-hoc diagnostic tool for operators and developers investigating workflow behavior:
1. **Root-Cause Analysis for Retries:** Inspecting the history of a task reveals the precise failure cause of each attempt (e.g., execution timeout vs. worker loss vs. unhandled exception) and the scheduled backoff timing.
2. **Arbitration Audit:** History entries document whether an execution result won the race against an execution timeout or user cancellation, providing unambiguous evidence of single-winner commit outcomes.
3. **Worker Attribution:** Every attempt history record correlates the exact `WorkerSessionId` that executed the task, enabling operators to identify misbehaving or failing worker nodes.
4. **Drain Progression Tracking:** During failure or cancellation drains, history entries illustrate the exact timeline of unstarted tasks cancelling, active attempts settling, and the root workflow terminalizing.
5. **Recovery Transparency:** State-altering recovery repairs emit explicit history entries detailing what state anomalies were corrected after an orchestrator crash.
6. **Separation from Telemetry:** History aids diagnosis of orchestration transitions but never overrides current state. Deep profiling, function call durations, and worker stdout/stderr logs remain in ADR-016.

---

## 16. Testing

All history test suites are planned requirements to be implemented during engine verification:
1. **Atomic Commit & Rollback Suite:** Verify that injecting a storage failure during history insert rolls back the entire state transition, and injecting a failure during state update writes zero history.
2. **One-Record-Per-Commit Verification Suite:** Verify that multi-entity consistency groups (Group 2, Group 4, Group 5, Group 6) emit exactly one semantic `HistoryEntry` with complete cross-entity references.
3. **Workflow Lifecycle Completeness Suite:** Verify that all nine workflow lifecycle transitions, including `INITIALIZING -> RUNNING`, generate their required history entries.
4. **Initial Task-Set Summarization Suite:** Verify that workflow initialization records task-set establishment in a single history entry rather than emitting $N$ duplicate entries.
5. **Recovery Repair Audit Suite:** Verify that state-altering recovery repairs generate audit entries, while routine clean recovery scans emit zero history.
6. **Worker-Loss Cause Verification Suite:** Verify that attempt failure due to worker loss records the semantic failure cause without creating illegal lifecycle states.
7. **Ephemeral Exclusion Suite:** Verify that worker heartbeats, routing candidate offers, rejected dispatches, and read-only API requests emit zero durable history.
8. **Parallel Task Independence Suite:** Verify that concurrent parallel tasks progress and emit history entries simultaneously without locking on a shared workflow sequence counter.
9. **Unknown-Commit Deduplication Suite:** Simulate network disconnects during commit; verify that subsequent re-reads resolve idempotently without duplicate history entries.
10. **Payload Non-Duplication Suite:** Inspect history storage for high-payload tasks; verify that business input and output payloads are never duplicated into history records.
11. **Purge Recovery Independence Suite:** Execute a workflow to completion; purge all its historical entries; verify that recovery sweeps, output queries, and state queries continue to function correctly.

---

## 17. Operational Considerations

### 17.1 Telemetry and Metrics (ADR-016)
The following operational metrics must be instrumented under [ADR-016](00-architecture-decision-register.md) to monitor the execution history subsystem:
- `history_entries_committed_total`: Total count of history entries committed, partitioned by semantic category.
- `history_write_failure_total`: Count of state persistence transactions aborted due to history write failures.
- `history_query_duration_seconds`: Latency of history query and pagination retrieval requests.
- `history_payload_bytes_total`: Volume of history metadata written per workflow execution.
- `history_purge_records_total`: Count of historical entries archived or purged by background retention jobs.
- `history_deduplication_suppressions_total`: Count of duplicate callback events that successfully avoided duplicate history writes.

### 17.2 Retention Policy
- Active execution history must be preserved until workflow terminalization (`SUCCEEDED`, `FAILED`, `CANCELLED`) to ensure complete operator visibility during in-flight operations.
- Post-terminal archival and purging are governed by operational policy (durations configured under [ADR-023](00-architecture-decision-register.md)).

### 17.3 Bounded Querying & Memory Protection
- The history query engine must enforce strict pagination and bounded result sets. The control plane must never attempt to load an entire unbounded workflow history into memory at once.

---

## 18. Maintenance

1. **Category Evolution:** New semantic history categories may be introduced as control-plane features expand, provided they follow the one-record-per-semantic-commit model and do not alter existing categories.
2. **Metadata Schema Evolution:** History diagnostic metadata must evolve in a backward-compatible manner (e.g., using additive, optional fields). Existing historical entries must remain readable without requiring data migration.
3. **Retention & Archival Jobs:** Periodic background jobs may archive or purge historical entries for terminal executions. These jobs must run with bounded transaction sizes to avoid database lock contention.
4. **Zero Migration Replay:** Database schema migrations must never rely on replaying historical events to rebuild or upgrade current-state tables.

---

## 19. Future Evolution

The following capabilities are out of scope for V1 but may build upon ADR-014 in future iterations:
1. **Compliance & Security Audit Tiers:** Integration with enterprise SIEM platforms, tamper-evident ledgers, or cryptographic verification chains ([ADR-022](00-architecture-decision-register.md)).
2. **Cold Storage Archival:** Automated offloading of historical entries for terminal workflows to secondary, low-cost object storage (e.g., S3/GCS) with on-demand retrieval.
3. **External Event Streaming:** Outbox-driven or CDC publication of historical events to message brokers (e.g., Kafka) for external analytics, decoupled from persistence transactions.
4. **Rich Timeline Visualizations:** Advanced Web UI timeline views providing Gantt charts, causal dependency graphs, and step-by-step playback derived from historical records.

---

## 20. Rejected Alternatives

1. **Application Logging as History (Candidate A):** Rejected because operational logs (stdout/stderr, file logs) are vulnerable to truncation, sampling, and rotation, failing to provide reliable, queryable, audit-grade history for users and operators.
2. **Event Sourcing as State Authority (Candidate B):** Rejected because deriving current state via event replay introduces severe operational complexity, slow restart recovery, event schema migration difficulties, and concurrency bottlenecks on parallel tasks.
3. **Asynchronous Best-Effort Audit Logging (Candidate C):** Rejected because a crash between state commit and asynchronous log writing permanently drops the audit record, creating unrecoverable gaps in execution timelines.
4. **Workflow-Wide Monotonic Sequence Counter:** Rejected because incrementing a shared counter on `WorkflowExecution` for every task event introduces severe lock contention, directly violating ADR-013's independent task concurrency model.
5. **Dual Split Streams (Security Audit vs. Execution History):** Rejected for V1 as unnecessary architectural duplication. Comprehensive user security auditing and IAM compliance belong to [ADR-022](00-architecture-decision-register.md).
6. **Cryptographic Tamper-Evident Hash Chains:** Rejected for V1 as an overengineered complexity. Standard relational/document storage transactions provide sufficient audit integrity for orchestration needs.

---

## 21. Decision Evolution

1. **Logs-Only Exploration:** Early discussions considered whether structured application logging (ADR-016) was sufficient for tracking past execution behavior. This was quickly rejected due to log rotation loss, lack of querying APIs, and the inability to guarantee audit durability.
2. **The Event-Sourcing Dilemma:** Event sourcing was evaluated as an attractive mechanism to unify history and state. However, deep analysis revealed that event replay fundamentally contradicted the fast, crash-resilient recovery goals of ADR-011 and ADR-012, while event stream serialization would destroy the parallel task concurrency designed in ADR-013.
3. **Asynchronous Logging Rejection:** Asynchronous event queues were considered to decouple history writes from state commits. This was rejected because orchestrator crashes during the asynchronous window would cause silent, unrecoverable audit record loss.
4. **Consensus on Atomic Append-Oriented History:** The final architecture established that current state must remain the sole authority, with history appended atomically within the same persistence transaction boundary, providing complete audit reliability without replay dependencies.

---

## 22. Common Misconceptions

- **Misconception: Because NexusFlow has an execution history log, it is an event-sourced engine.**
  *Reality:* NexusFlow is strictly a durable current-state engine. History is an append-only explanatory log created atomically on the write path; it is never replayed to reconstruct current state.
- **Misconception: Atomic history commit makes history a correctness authority.**
  *Reality:* Atomicity is a write-path durability guarantee (ensuring state changes always have audit records). On the read path, schedulers and recovery routines never consult history.
- **Misconception: Control-plane commit timestamps establish authoritative concurrency ordering.**
  *Reality:* Control-plane recorded timestamps are the trusted timestamp source for audit metadata, but timestamps themselves are not the semantic ordering authority. Semantic ordering derives from legal lifecycle transitions, revisions, Attempt ordinals, and ADR-013 single-winner commit outcomes.
- **Misconception: History entries must be totally ordered across the entire workflow.**
  *Reality:* Independent parallel tasks have no causal relationship. Forcing a total order creates a major bottleneck on the workflow root entity. Causal ordering per entity and deterministic tie-breaking for presentation are completely sufficient.
- **Misconception: Deleting old history records will break orchestrator recovery.**
  *Reality:* Crash recovery reconciles normalized current state directly. Deleting or archiving historical entries has zero impact on recovery correctness.
- **Misconception: Worker-reported timestamps represent when a task executed.**
  *Reality:* Worker clocks are subject to skew and drift. Only control-plane recorded commit timestamps (`committed_at`) carry authority.

---

## 23. Open Questions

- *None.* All architectural boundaries, authority separations, consistency-group mappings, data minimization rules, and ordering models are fully resolved.

---

## 24. Interview Discussion

### Key Architectural Concepts to Defend
1. **Why Durable Current State + History Instead of Event Sourcing?**
   - In orchestration systems, scheduling, dependency checking, and retry budgeting require direct, indexed access to the *current* state of tasks and workflows. Replaying hundreds of events to answer "is task B ready to run?" adds massive latency and CPU overhead. NexusFlow maintains normalized current state as the sole operational truth, appending history atomically purely as an immutable audit and diagnostic trail.
2. **Why Atomic Write-Path Commitment Without Read-Path Dependency?**
   - Atomicity ensures that an auditable state transition cannot commit without its historical proof (preventing missing audit records) and that an uncommitted transaction writes no history (preventing phantom audit records). However, schedulers and recovery routines never query history. This decouples the engine's operational loops from the volume or schema evolution of historical logs.
3. **How Does ADR-014 Avoid Violating ADR-013 Concurrency?**
   - If every task transition in a wide DAG had to increment a shared `WorkflowExecution.history_sequence` counter, parallel DAG execution would collapse into a serialized bottleneck at the database. ADR-014 explicitly rejects workflow-wide sequence counters. Ordering is causal per entity (via revisions and ordinals), while presentation ordering relies on control-plane timestamps and storage tie-breakers.
4. **Why Are Worker Clocks Untrusted?**
   - In distributed systems, worker clocks suffer from drift, NTP jumps, and potential malicious manipulation. Relying on worker timestamps for timeouts, ordering, or retries introduces non-deterministic race conditions. NexusFlow establishes all authoritative deadlines and commit timestamps strictly on the control plane.
5. **Why Are Payloads Excluded from History?**
   - Tasks may process megabytes of JSON or binary data. Copying input and output payloads into every history transition would cause explosive storage growth and increase transaction latency. Authoritative payloads reside in dedicated current-state tables; history entries store only references and commit confirmations.

---

## 25. References

- [ADR-001: Internal Workflow Specification](adr-001-internal-workflow-specification.md)
- [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md)
- [ADR-006: WorkflowExecution State Machine](adr-006-workflow-execution-state-machine.md)
- [ADR-007: TaskExecution Lifecycle & Attempt Model](adr-007-task-execution-lifecycle-and-attempt-model.md)
- [ADR-008: Worker Coordination & Liveness Model](adr-008-worker-coordination-and-liveness-model.md)
- [ADR-009: Task Routing Strategy](adr-009-task-routing-strategy.md)
- [ADR-010: Workflow Data Flow & Parameter Passing](adr-010-workflow-data-flow-and-parameter-passing.md)
- [ADR-011: State Persistence Strategy](adr-011-state-persistence-strategy.md)
- [ADR-012: Recovery Strategy](adr-012-recovery-strategy.md)
- [ADR-013: Consistency & Concurrency Strategy](adr-013-consistency-and-concurrency-strategy.md)
- [00-Architecture Decision Register](00-architecture-decision-register.md)

---

## 26. Traceability

| Requirement | Architectural Component | Section Reference |
| :--- | :--- | :--- |
| **FR-HIST-001** | Comprehensive Lifecycle Event Coverage | Section 10.3, 10.4 |
| **FR-HIST-002** | Semantic Causal Context Capture | Section 10.2, 10.4, 15 |
| **FR-HIST-003** | Atomic State + History Commit | Section 10.1 (#3, #4), 11.2 |
| **FR-HIST-004** | Duplicate History Prevention | Section 10.1 (#6), 14 (#5, #6) |
| **FR-HIST-005** | Bounded Query & Entity Correlation | Section 10.2, 17.3 |
| **NFR-COR-002**| Authority Separation (No Replay) | Section 10.1 (#1, #2), 11.1 |
| **NFR-SCA-002**| Concurrency Preservation (No Hot Counter) | Section 10.1 (#11), 11.4 |
| **NFR-DAT-001**| Payload Minimization | Section 10.1 (#8), 17 |
| **NFR-REL-003**| Retention Decoupling from Correctness | Section 10.1 (#12), 18 |

---

## 27. Decision Validation Checklist

- [x] **Standard 27-section ADR structure strictly preserved.**
- [x] **Atomic Append-Oriented Execution History selected as core model.**
- [x] **Current state established as sole source of truth for scheduling and recovery.**
- [x] **History replay explicitly prohibited for state reconstruction.**
- [x] **Required state mutation and HistoryEntry commit within the same atomic persistence transaction.**
- [x] **History write failure fails closed, preventing un-audited state transitions.**
- [x] **History read failure does not block scheduling or crash recovery.**
- [x] **One HistoryEntry per semantic consistency-group commit.**
- [x] **Complete workflow lifecycle recorded, including `INITIALIZING -> RUNNING`.**
- [x] **Initial TaskExecution set establishment summarized without $N$ redundant entries.**
- [x] **All Task/Attempt lifecycle consistency groups (Groups 1–6, cancellation) represented accurately.**
- [x] **Worker loss treated as a semantic failure cause, not an illegal lifecycle state.**
- [x] **Ephemeral operational noise (heartbeats, routing offers, lost OCC races, no-ops) strictly excluded.**
- [x] **Business input and output payload duplication strictly prohibited.**
- [x] **Sensitive credentials and raw stack traces excluded from history metadata.**
- [x] **History entries are append-only and immutable during retention lifetime.**
- [x] **No global or workflow-wide sequence counter required, preserving ADR-013 concurrency.**
- [x] **Timestamps are descriptive metadata; worker clocks are untrusted.**
- [x] **History purge does not affect recovery, scheduling, or output resolution under ADR-011/012.**
- [x] **Active execution history preserved until terminalization for auditability.**
- [x] **Corrupted history is never replayed into state; integrity faults handled fail-safe.**
- [x] **Strict boundaries maintained with ADR-015, ADR-016, ADR-018, ADR-020, ADR-022, and ADR-023.**
- [x] **No unsupported performance claims or absolute hyperbole.**
