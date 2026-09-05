# ADR-006 — Workflow Execution State Machine

*   **Status**: Approved
*   **Last Updated**: 2026-09-05
*   **Deciders**: Project Owner
*   **Domain**: Execution (Workflow Lifecycle & State Machine)
*   **Criticality**: Critical
*   **Relationships**:
    *   **Depends On**: [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md), [ADR-004: Workflow Validation Strategy](adr-004-workflow-validation-strategy.md), [ADR-005: Workflow Task Scheduling & Dispatch Architecture](adr-005-workflow-task-scheduling-and-dispatch-architecture.md)
    *   **Enables**: [ADR-007: Task Execution Lifecycle & Attempt Model](00-architecture-decision-register.md#L202)
    *   **Related To**: [ADR-008: Worker Coordination & Liveness Model](00-architecture-decision-register.md#L203), [ADR-010: Workflow Data Flow & Parameter Passing](00-architecture-decision-register.md#L209), [ADR-011: State Persistence Strategy](00-architecture-decision-register.md#L210), [ADR-012: Recovery Strategy](00-architecture-decision-register.md#L211), [ADR-013: Consistency & Concurrency Strategy](00-architecture-decision-register.md#L212), [ADR-014: History & Audit Model](00-architecture-decision-register.md#L213), [ADR-015: HTTP & REST API Layer](00-architecture-decision-register.md#L214), [ADR-017: Graceful Shutdown Strategy](00-architecture-decision-register.md#L214)

---

## 1. Purpose
This document defines the authoritative lifecycle state machine for a Workflow Execution in NexusFlow. It establishes how a workflow transitions from instantiation into active execution, gates task schedulability, reacts to task failures and external cancellation requests, manages directional draining of active work, enforces terminal-state immutability, and aggregates child task outcomes into a final workflow outcome. It ensures that workflow progression is deterministic, consistent under concurrency, and fully reconcilable across process crashes.

---

## 2. Context
NexusFlow establishes workflow definitions through [ADR-001](adr-001-internal-workflow-specification.md) (IWS) and derives immutable, bidirectional dependency topology through [ADR-003](adr-003-canonical-workflow-graph-representation.md). [ADR-004](adr-004-workflow-validation-strategy.md) validates topological acyclicity, entity uniqueness, and reference integrity. At runtime, [ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md) governs task-level progression, establishing that:
1. All logical `TaskExecution` entities are eagerly instantiated at workflow start.
2. The scheduler progression check is conditioned on an abstract predicate: `workflow_allows_progression(W)`.
3. The scheduler progresses tasks when dependencies are satisfied, but does not own workflow-level completion, cancellation, or failure aggregation.

Without a disciplined top-level lifecycle state machine:
* Systems fall into inconsistent terminal states where a workflow is marked failed or cancelled while active workers continue executing tasks in isolation.
* External cancellation commands race with simultaneous task completions, creating non-deterministic outcomes.
* Schedulers continue dispatching new tasks even after an unrecoverable task failure has doomed the workflow.
* Recovery systems cannot determine whether an active workflow was mid-initialization, progressing normally, or draining work prior to a crash.

ADR-006 defines the authoritative Workflow Execution state machine that fulfills the scheduler's schedulability contract and governs workflow convergence.

---

## 3. Problem Statement
How should NexusFlow represent and transition the lifecycle of a Workflow Execution so that scheduling, cancellation, task outcomes, failure propagation, recovery, and terminal-state determination remain deterministic, durable, and consistent under concurrent events?

---

## 4. Requirements Covered
*   **Capability 6**: Workflow Execution Lifecycle Management.
*   **System Invariant (KT-8)**: "A workflow execution must reach exactly one terminal state (SUCCEEDED, FAILED, CANCELLED)."
*   **System Invariant (KT-8)**: "Terminal workflow states must never transition to non-terminal states."
*   **System Invariant (KT-8)**: "Workflow output is valid only after successful workflow completion."
*   **Core Principle**: Correctness Over Performance.
*   **Core Principle**: Explicit Behaviour Over Implicit Behaviour.
*   **Core Principle**: Deterministic State Transitions.
*   **Core Principle**: Recovery as a First-Class Capability.
*   **Core Principle**: Clear Ownership and Separation of Responsibilities.

---

## 5. Constraints
*   **Scheduler Schedulability Gating**: The state machine must directly satisfy ADR-005's requirement that only an active workflow permits task progression.
*   **Eager Task Execution Compatibility**: The lifecycle must accommodate the eager instantiation of all child `TaskExecution` entities established in ADR-005.
*   **Logical Settlement over Remote Physical Guarantees**: Invariant enforcement must be based on authoritative engine state, acknowledging that physical remote worker processes cannot be guaranteed to halt synchronously across network boundaries.
*   **Single-Developer V1 Feasibility**: Avoid distributed consensus, complex statechart DSLs, multi-phase commit protocols, or saga compensation engines.
*   **Technology Independence**: The state machine must remain decoupled from specific database engines, transaction mechanisms, message brokers, and transport protocols.

---

## 6. Goals
*   Adopt a 7-state directional drain lifecycle model: `INITIALIZING`, `RUNNING`, `FAILING`, `CANCELLING`, `SUCCEEDED`, `FAILED`, `CANCELLED`.
*   Formally bind the schedulability predicate such that `workflow_allows_progression(W)` evaluates to `true` strictly when status is `RUNNING`.
*   Enforce a fail-fast policy for new work combined with best-effort cancellation intent for active work upon fatal task failures.
*   Establish that a workflow may enter a terminal state (`SUCCEEDED`, `FAILED`, `CANCELLED`) if and only if all child `TaskExecution` records have logically converged to terminal task-level outcomes.
*   Guarantee terminal-state immutability and monotonic state progression.
*   Define single-winner atomic transition semantics for resolving races between concurrent lifecycle events (success, failure, cancellation).
*   Ensure that Workflow Execution lifecycle state is authoritative and fully compatible with crash recovery ([ADR-012](00-architecture-decision-register.md#L211)) without requiring a dedicated `RECOVERING` state.

---

## 7. Non-Goals
*   Defining concrete `TaskExecution` state names, attempt state machines, or retry backoff algorithms (deferred to [ADR-007](00-architecture-decision-register.md#L202)).
*   Selecting worker cancellation delivery protocols, worker heartbeat mechanisms, or worker liveness tracking (deferred to [ADR-008](00-architecture-decision-register.md#L203)).
*   Defining execution input payload validation, output schema enforcement, or parameter passing (deferred to [ADR-010](00-architecture-decision-register.md#L209)).
*   Selecting database engines, SQL table schemas, or persistence transaction mechanisms (deferred to [ADR-011](00-architecture-decision-register.md#L210)).
*   Designing recovery scanning loops, boot-up reconciliation sweeps, or crash detection algorithms (deferred to [ADR-012](00-architecture-decision-register.md#L211)).
*   Selecting concurrency control mechanisms such as conditional compare-and-swap (CAS), row locks, or isolation levels (deferred to [ADR-013](00-architecture-decision-register.md#L212)).
*   Designing execution history event persistence schemas (deferred to [ADR-014](00-architecture-decision-register.md#L213)).
*   Designing HTTP REST status codes, JSON response envelopes, or endpoint routes (deferred to [ADR-015](00-architecture-decision-register.md#L214)).
*   Designing engine graceful shutdown procedures or draining timeouts (deferred to [ADR-017](00-architecture-decision-register.md#L214)).
*   Supporting workflow-level execution timeouts, pause/resume, or manual intervention in V1.

---

## 8. Candidate Solutions

### Alternative A: Minimal Lifecycle (No Draining States)
A simple 4-state lifecycle: `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`.
*   *Mechanism*: Workflows transition immediately from `RUNNING` to `FAILED` upon task failure, or to `CANCELLED` upon cancellation.
*   *Pros*: Minimal implementation complexity.
*   *Cons*: Terminal workflows coexist with active worker tasks, causing late-arriving task completions to corrupt dead workflows; API callers observe terminal states while work is still executing; violates the terminal cleanliness invariant.

### Alternative B: Cancellation-Only Drain Lifecycle
A 6-state lifecycle: `INITIALIZING`, `RUNNING`, `CANCELLING`, `SUCCEEDED`, `FAILED`, `CANCELLED`.
*   *Mechanism*: Introduces `CANCELLING` to drain work after user cancellation, but fails workflows immediately upon task failure.
*   *Pros*: Handles user cancellation cleanly.
*   *Cons*: Asymmetric failure handling; failing workflows still suffer from active sibling tasks executing after the workflow is marked `FAILED`.

### Alternative C: Directional Drain State Machine [Selected]
A 7-state lifecycle: `INITIALIZING`, `RUNNING`, `FAILING`, `CANCELLING`, `SUCCEEDED`, `FAILED`, `CANCELLED`.
*   *Mechanism*: Differentiates active execution (`RUNNING`), failure-directed draining (`FAILING`), and cancellation-directed draining (`CANCELLING`). Schedulability is revoked immediately upon entering either draining state. Transitions to terminal states require all child tasks to logically settle.
*   *Pros*: Enforces strict terminal cleanliness; symmetric handling of failure and cancellation; clear recovery semantics; robust under concurrency.
*   *Cons*: Requires two intermediate draining states and explicit task-settling logic.

### Alternative D: Purely Derived Workflow Lifecycle
The engine stores no authoritative workflow lifecycle state. The workflow's status is dynamically computed on demand from the set of child task execution records.
*   *Mechanism*: Aggregates task records on every query.
*   *Pros*: Avoids maintaining a separate workflow-level state field.
*   *Cons*: Cannot capture external cancellation intent (cancellation is an administrative workflow-level command, not a task property); highly vulnerable to read races and dynamic inconsistency during task transitions; poor recovery anchoring.

### Alternative E: Generic Terminating Lifecycle
A 6-state lifecycle: `INITIALIZING`, `RUNNING`, `TERMINATING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, where `TERMINATING` carries a metadata reason (`FAILURE` vs. `CANCELLATION`).
*   *Mechanism*: Uses a single draining state.
*   *Pros*: Consolidates draining logic into a single state.
*   *Cons*: Obscures failure versus cancellation intent in secondary metadata; creates ambiguity in API status reporting; requires conditional checks to determine whether the terminal destination is `FAILED` or `CANCELLED`.

---

## 9. Detailed Evaluation of Candidate Solutions

| Evaluation Dimension | Alt A: Minimal | Alt B: Cancel-Only Drain | Alt C: Directional Drain (Selected) | Alt D: Purely Derived | Alt E: Generic Terminating |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Terminal Invariant** | Violated (active tasks in terminal) | Violated on failure | **Preserved strictly** | Undefined | Preserved strictly |
| **Schedulability Control** | Weak | Asymmetric | **Optimal** (immediate revocation) | Dynamic/Fragile | Optimal |
| **Operational Clarity** | Poor (hidden active work) | Medium | **Highest** (clear directional intent) | Lowest | Medium (metadata-dependent) |
| **Crash Recovery** | Weak (cannot resume drain) | Partial | **Robust** (resumes exact intent) | Poor (race hazards) | Medium |
| **Implementation Complexity** | Lowest | Medium | **Balanced** (V1 appropriate) | High (aggregation races) | Balanced |
| **Solo-Developer Feasibility**| High | High | **High** | Medium | High |

---

## 10. Decision
NexusFlow adopts **Alternative C: Directional Drain State Machine** as the authoritative Workflow Execution lifecycle architecture.

### 10.1 Authoritative Workflow Execution State Set
The concrete, authoritative Workflow Execution lifecycle states are:
$$\text{States} = \{\text{INITIALIZING}, \text{RUNNING}, \text{FAILING}, \text{CANCELLING}, \text{SUCCEEDED}, \text{FAILED}, \text{CANCELLED}\}$$

```
                [Start Request Accepted]
                           │
                           ▼
                    ┌──────────────┐
                    │ INITIALIZING │
                    └──────┬───────┘
                           │
             ┌─────────────┼─────────────────────────┐
             │             │                         │
      (Initialization      │ (All Runtime Entities   │ (External Cancel
       Unrecoverable       │  Established Durably)   │  Accepted)
       Semantic Error)     ▼                         ▼
             │      ┌──────────────┐          ┌──────────────┐
             │      │   RUNNING    │          │  CANCELLING  │
             │      └──────┬───────┘          └──────┬───────┘
             │             │                         │
             │      ┌──────┴──────────────┐          │
             │      │                     │          │
             │ (Fatal Task         (All Tasks        │
             │  Failure)            Succeeded)       │
             ▼      ▼                     ▼          │
      ┌──────────────┐             ┌──────────────┐  │
      │   FAILING    │             │  SUCCEEDED   │  │
      └──────┬───────┘             │  (Terminal)  │  │
             │                     └──────────────┘  │
             │ (All Tasks                            │ (All Tasks
             │  Logically                            │  Logically
             │  Terminal)                            │  Terminal)
             ▼                                       ▼
      ┌──────────────┐                        ┌──────────────┐
      │    FAILED    │                        │  CANCELLED   │
      │  (Terminal)  │                        │  (Terminal)  │
      └──────────────┘                        └──────────────┘
```

### 10.2 State Semantics and Categorization

| State | Category | Schedulable? | Terminal? | Meaning & Operational Scope |
| :--- | :--- | :---: | :---: | :--- |
| **INITIALIZING** | Active | No | No | Root `WorkflowExecution` entity exists; child `TaskExecution` entities are being established durably. Schedulability disabled. |
| **RUNNING** | Active | **Yes** | No | Normal execution state. Schedulability enabled. Sibling and dependent tasks progress under ADR-005. |
| **FAILING** | Draining | No | No | Failure-directed non-terminal state. Schedulability revoked. Best-effort cancellation issued to active tasks. Awaiting logical settlement. |
| **CANCELLING**| Draining | No | No | Cancellation-directed non-terminal state. Schedulability revoked. Best-effort cancellation issued to active tasks. Awaiting logical settlement. |
| **SUCCEEDED** | Terminal | No | **Yes** | 100% of child `TaskExecution` records have reached terminal success. Successful workflow output is valid. Immutable. |
| **FAILED** | Terminal | No | **Yes** | Workflow terminated unsuccessfully. All child tasks logically settled. No valid workflow output. Immutable. |
| **CANCELLED** | Terminal | No | **Yes** | Workflow terminated by accepted external cancellation. All child tasks logically settled. No valid workflow output. Immutable. |

### 10.3 Legal Transition Matrix

| Source State | Target State | Trigger / Guard Condition |
| :--- | :--- | :--- |
| **INITIALIZING** | **RUNNING** | All required runtime execution entities (including eager `TaskExecution` records) established consistently. |
| **INITIALIZING** | **CANCELLING** | External cancellation request durably accepted after `WorkflowExecution` entity exists. |
| **INITIALIZING** | **FAILED** | Definitive, unrecoverable execution-initialization failure encountered after entity exists. |
| **RUNNING** | **SUCCEEDED** | 100% of required child `TaskExecution` entities have reached terminal successful outcomes. |
| **RUNNING** | **FAILING** | Any child `TaskExecution` reaches a definitive workflow-failing terminal outcome. |
| **RUNNING** | **CANCELLING** | External cancellation command durably accepted. |
| **FAILING** | **FAILED** | 100% of child `TaskExecution` entities have reached terminal task-level outcomes. |
| **CANCELLING** | **CANCELLED** | 100% of child `TaskExecution` entities have reached terminal task-level outcomes. |

*Any transition not explicitly listed in this table is illegal.*

### 10.4 Schedulability Contract with ADR-005
The scheduler's abstract progression predicate is formally defined as:
$$\text{workflow\_allows\_progression}(W) \iff \text{status}(W) == \text{RUNNING}$$
*   In `INITIALIZING`, `FAILING`, `CANCELLING`, `SUCCEEDED`, `FAILED`, and `CANCELLED`, new task progression is strictly prohibited.
*   *Distinction*: Non-schedulability forbids starting new work, but does **not** reject incoming outcome reports from already-running tasks. In `FAILING` and `CANCELLING`, active workers may still report completions or failures, which are recorded at the task level.

### 10.5 Initialization Semantics
1.  **Entity Establishment**: Instantiation begins in `INITIALIZING` to establish durable workflow state and eagerly create all child `TaskExecution` entities (per ADR-005).
2.  **Transition to `RUNNING`**: Occurs when entity creation succeeds. Root task evaluation is **not** a prerequisite for `INITIALIZING -> RUNNING`; initial scheduling occurs under ADR-005 once the workflow is `RUNNING`.
3.  **Crash Mid-Initialization**: If an orchestrator process crashes during `INITIALIZING`, the workflow is **not** failed. Startup recovery ([ADR-012](00-architecture-decision-register.md#L211)) inspects the record and either completes instantiation or cleans it up.
4.  **`INITIALIZING -> FAILED` Boundary**: Reserved exclusively for definitive, unrecoverable semantic initialization failures (e.g., malformed execution context parameters). Transient infrastructure outages (database disconnects, broker drops) must never trigger workflow failure.

### 10.6 Fail-Fast Policy for New Work & Best-Effort Active Cancellation
Upon detecting a definitive workflow-failing task outcome while in `RUNNING`:
1.  The workflow immediately executes an atomic transition from `RUNNING -> FAILING`.
2.  Schedulability is revoked immediately; pending and pre-runnable tasks will never be scheduled.
3.  The engine emits abstract best-effort cancellation intent to all currently active child tasks via ADR-007/008.
4.  Un-started tasks are resolved into terminal non-executable outcomes by ADR-007.
5.  Active tasks are allowed to complete, fail, or be cancelled by workers. Once all child tasks reach terminal task-level outcomes, the workflow transitions `FAILING -> FAILED`.

### 10.7 External Cancellation Semantics
Upon durably accepting an external cancellation command:
1.  If `RUNNING` or `INITIALIZING`, the workflow transitions to `CANCELLING`.
2.  Schedulability is revoked immediately.
3.  Best-effort cancellation intent is emitted to all active child tasks via ADR-007/008.
4.  Un-started tasks are resolved into terminal non-executable outcomes by ADR-007.
5.  Once all child tasks reach terminal outcomes, the workflow transitions `CANCELLING -> CANCELLED`.

### 10.8 Terminal Task Convergence Invariant
A Workflow Execution may commit a transition to `SUCCEEDED`, `FAILED`, or `CANCELLED` if and only if **every** child `TaskExecution` associated with the workflow definition has reached a terminal task-level outcome.
*   **Logical Settlement Defined**: The engine's authoritative records confirm that no child task remains capable of valid future execution, scheduler progression, or dispatch within that workflow lifecycle.
*   **Physical Boundary**: Logical settlement does not require proof that an unreachable or partitioned remote worker process has ceased executing instructions. Unresponsive active work is authoritatively settled via timeout and liveness policies defined in ADR-007/008.

### 10.9 Terminal Immutability & Monotonicity
1.  `SUCCEEDED`, `FAILED`, and `CANCELLED` are permanently terminal. No legal transition originates from a terminal state. Late-arriving callbacks cannot mutate a terminal workflow record.
2.  Progression is strictly monotonic: once a workflow enters `FAILING` or `CANCELLING`, it cannot return to `RUNNING`. `FAILING` can transition only to `FAILED`; `CANCELLING` can transition only to `CANCELLED`.

### 10.10 Single-Winner Transition Semantics & Race Resolution
If incompatible lifecycle transitions race from the same authoritative workflow state, exactly one transition may commit:
*   **Success vs. Cancellation**: If `RUNNING -> SUCCEEDED` commits first, the workflow is immutable; subsequent cancellation attempts observe a terminal state and perform no mutation. If `RUNNING -> CANCELLING` commits first, the workflow is cancellation-directed; late task successes are recorded at the task level, but the workflow proceeds to `CANCELLED`.
*   **Failure vs. Cancellation (First-Committed Direction Rule)**: If `RUNNING -> FAILING` commits first, the workflow is failure-directed and will terminate in `FAILED`; a concurrent cancellation request does not alter this lifecycle direction. If `RUNNING -> CANCELLING` commits first, the workflow is cancellation-directed and will terminate in `CANCELLED`; a concurrent task failure does not redirect it to `FAILED`.

### 10.11 Runnable-but-Not-Dispatched Task Safety
If a `TaskExecution` has advanced to an execution-eligible/runnable condition under ADR-005, but the containing workflow transitions to `FAILING` or `CANCELLING` before worker dispatch occurs, that task must **not** be dispatched. Downstream dispatch and claim operations ([ADR-008](00-architecture-decision-register.md#L203) / [ADR-013](00-architecture-decision-register.md#L212)) must verify that the workflow is in `RUNNING` status at the moment of dispatch.

### 10.12 Authoritative Durable State Model
Workflow Execution lifecycle state is authoritative state stored in durable persistence. It is not dynamically derived from task states, because states such as `CANCELLING` and `FAILING` encode external intent and directional trajectories that cannot be inferred from child tasks alone.

---

## 11. Decision Rationale
1. **Clean Operational Boundaries and Resource Preservation**: Immediate fail-fast revocation of schedulability prevents the engine from scheduling independent branches once workflow failure is inevitable. Issuing best-effort cancellation intent to active sibling tasks halts unnecessary compute and worker resource consumption.
2. **Elimination of Zombie Completion Corruption**: Requiring full terminal task convergence before transitioning to `FAILED` or `CANCELLED` prevents active tasks from reporting outcomes back to dead workflows. The intermediate `FAILING` and `CANCELLING` states provide a clear, auditable settling window.
3. **Robustness Against Process Crashes**: Having explicit `INITIALIZING`, `FAILING`, and `CANCELLING` states allows startup recovery ([ADR-012](00-architecture-decision-register.md#L211)) to deduce the exact operational intent of interrupted workflows without complex heuristic guessing.
4. **Deterministic Race Resolution**: The first-committed direction rule and single-winner atomic transition contract eliminate race conditions between external commands and internal completion events without relying on fragile wall-clock timestamps.
5. **Truthful Separation of Logical Settlement from Physical Certainty**: Recognizing that network partitions make instantaneous physical termination impossible ensures that the engine's core invariants remain enforceable in real-world distributed environments.

---

## 12. Tradeoffs
*   **Intermediate Draining Overhead vs. Immediate Termination**: We sacrifice immediate terminalization on failure or cancellation in exchange for guaranteed terminal task convergence and audit integrity.
*   **Fail-Fast vs. Independent Branch Completion**: In workflows with disconnected components, we choose to halt all branches upon the first fatal task failure rather than allowing independent branches to finish, prioritizing simpler failure semantics and reduced compute waste.
*   **Authoritative Workflow State vs. Pure Derivation**: Storing an explicit workflow state introduces a database column and transition logic, but provides unambiguous tracking of external cancellation and failure-directed draining.

---

## 13. Consequences

### Codebase & Architectural Impact
*   **Scheduler Coordination ([ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md))**: The scheduler cleanly checks `status == RUNNING` before progressing tasks and relies on eager task creation in `INITIALIZING`.
*   **Task Lifecycle Decoupling ([ADR-007](00-architecture-decision-register.md#L202))**: ADR-006 communicates via abstract task categories (*terminal successful*, *workflow-failing*, and *terminal non-executable*), leaving task-level attempt counters and enum names to ADR-007.
*   **Worker Decoupling ([ADR-008](00-architecture-decision-register.md#L203))**: ADR-006 emits abstract cancellation intent without prescribing transport protocols, queues, or RPC mechanisms.
*   **Persistence & Concurrency ([ADR-011](00-architecture-decision-register.md#L210) / [ADR-013](00-architecture-decision-register.md#L212))**: Requires an atomic single-winner transition mechanism to enforce legal state transitions under concurrency.

### Performance Impact
*   Workflow state transitions occur strictly at lifecycle milestones (initialization, failure, cancellation, completion); they are not evaluated on high-frequency dispatch loops.
*   Terminal aggregation reads authoritative task records only upon task completion events. Derived counters may optimize these checks without becoming the sole source of truth.

---

## 14. Failure Modes

| Failure Mode | Symptom / Consequence | Owning ADR | Mitigation |
| :--- | :--- | :--- | :--- |
| **Crash During `INITIALIZING`** | Workflow record exists, but child task records are partially written | ADR-006 / ADR-012 | Startup recovery inspects `INITIALIZING`; either completes eager task instantiation or marks initialization failed closed. |
| **Crash Post-`RUNNING`, Pre-Roots** | Workflow is `RUNNING`, but no tasks have been scheduled | ADR-005 / ADR-006 / ADR-012 | Schedulability is active; ADR-005 reconciliation evaluates root tasks and triggers progression. |
| **Fatal Task Failure in Parallel Branch** | Task in branch $A$ fails permanently while branch $B$ runs | ADR-006 | Fail-fast: Workflow transitions `RUNNING -> FAILING`, halting branch $B$ scheduling and issuing cancellation to active $B$ tasks. |
| **Simultaneous Success and Cancel** | Last task succeeds at exact instant cancellation arrives | ADR-006 / ADR-013 | Single-winner atomic transition: whichever commits first (`SUCCEEDED` vs `CANCELLING`) establishes immutable outcome; loser becomes no-op. |
| **Simultaneous Failure and Cancel** | Task permanently fails at exact instant cancellation arrives | ADR-006 / ADR-013 | Single-winner atomic transition: first to commit (`FAILING` vs `CANCELLING`) determines terminal destination (`FAILED` vs `CANCELLED`). |
| **Runnable Task During Draining** | Task becomes runnable right as workflow enters `FAILING`/`CANCELLING` | ADR-006 / ADR-008 / ADR-013 | Downstream dispatch/claim operations verify workflow `status == RUNNING`; dispatch is aborted if non-schedulable. |
| **Worker Never Acknowledges Cancel** | Active task attempt hangs during workflow draining | ADR-006 / ADR-007 / ADR-008 | ADR-007/008 liveness and timeout mechanisms declare the task logically settled, unblocking workflow terminalization. |
| **Late Callback to Terminal Workflow** | Worker reports task completion after workflow is terminal | ADR-006 / ADR-014 | Terminal immutability: late event cannot mutate workflow state. Event is logged/audited without altering workflow outcome. |
| **Unstarted Tasks Left Pending** | Descendants of failed task prevent failure aggregation | ADR-006 / ADR-007 | Upon entering `FAILING` or `CANCELLING`, ADR-007 marks un-started tasks as terminal non-executable, driving active count to zero. |
| **Crash During `FAILING` or `CANCELLING`** | Engine dies while draining in-flight work | ADR-006 / ADR-012 | Recovery resumes drain evaluation; re-issues cancellation signals to any unresolved active attempts. |
| **Transient Dispatch Infrastructure Outage** | Worker pool empty or broker queue temporarily unavailable | ADR-005 / ADR-006 | Workflow remains `RUNNING`; dispatch outage is an operational interruption, not a workflow failure. |
| **Derived Counter Drift** | In-memory task count cache diverges from database records | ADR-006 | Counters are non-authoritative; completion aggregation verifies state against durable `TaskExecution` records. |

---

## 15. Debugging Considerations
*   **State Trajectory Inspection**: When an execution is non-terminal, operators can inspect `status` (`INITIALIZING`, `RUNNING`, `FAILING`, `CANCELLING`) to immediately understand whether the workflow is actively scheduling or draining work.
*   **Settlement Diagnostics**: For workflows in `FAILING` or `CANCELLING`, diagnostics must expose which specific child `TaskExecution` instances remain non-terminal, highlighting blocking workers or unresolved tasks.
*   **Auditable Transition History**: Every lifecycle transition must be recorded with its causal trigger (e.g., `RUNNING -> FAILING caused by permanent failure of task 'payment_charge'`) under [ADR-014](00-architecture-decision-register.md#L213).

---

## 16. Testing Considerations
Lifecycle testing must verify the following scenarios (coordinated with [ADR-021](00-architecture-decision-register.md#L220)):
1.  **Happy Path Progression**: `INITIALIZING -> RUNNING -> SUCCEEDED` across single-task, linear chain, fan-out/fan-in, and disconnected DAG topologies.
2.  **Failure Draining**: `INITIALIZING -> RUNNING -> FAILING -> FAILED`, verifying that new scheduling halts immediately and all unstarted tasks settle as terminal non-executable.
3.  **Cancellation Draining**: `INITIALIZING -> RUNNING -> CANCELLING -> CANCELLED`, verifying best-effort cancellation of active tasks and settlement of unstarted work.
4.  **Early Cancellation**: `INITIALIZING -> CANCELLING -> CANCELLED` when cancellation is accepted before initialization completes.
5.  **Unrecoverable Initialization Failure**: `INITIALIZING -> FAILED` upon definitive semantic startup error.
6.  **Concurrency Races**: Concurrent execution of success vs. cancellation, failure vs. cancellation, and task progression vs. draining transitions, verifying single-winner atomic commits.
7.  **Terminal Immutability**: Late-arriving task completion reports submitted to `SUCCEEDED`, `FAILED`, and `CANCELLED` workflows, asserting zero state mutation.
8.  **Recovery Re-Observation**: Simulating engine reboot across each non-terminal state (`INITIALIZING`, `RUNNING`, `FAILING`, `CANCELLING`), asserting correct resumption of lifecycle rules without adding a `RECOVERING` state.

---

## 17. Operational Considerations
*   **Decoupled from Graceful Shutdown**: Graceful engine shutdown ([ADR-017](00-architecture-decision-register.md#L214)) does not cancel or fail workflows; non-terminal workflows remain durably stored, ready to resume on restart.
*   **Zero In-Memory Traps**: Because workflow lifecycle state and task records are persisted durably, orchestrator processes can be terminated or restarted without stranding workflows.
*   **Fail-Fast Compute Savings**: Immediately stopping new task progression upon fatal failures prevents resource exhaustion on doomed workflow executions.

---

## 18. Maintenance Considerations
*   **Extending Schedulability**: If pause/resume capabilities are introduced in future revisions, a `PAUSED` state can be integrated without modifying downstream task execution logic.
*   **Adding Conditional Branches**: If skip or conditional semantics are added under future ADRs, they integrate by expanding ADR-007's terminal task classifications without restructuring the 7-state workflow machine.

---

## 19. Future Evolution
*   **Workflow-Level Execution Timers (V2)**: If global execution timeouts are introduced, a timer trigger can initiate a transition `RUNNING -> FAILING` (or to a specialized `TIMED_OUT` state if justified).
*   **Pause and Resume (V2)**: A `PAUSED` state may be introduced between `INITIALIZING` and `RUNNING` or branched from `RUNNING`.
*   **Partial Success and Error Handling Policies (V2)**: Policy extensions allowing workflows to continue after certain task failures can be introduced by refining the workflow-failing task classification.

---

## 20. Rejected Alternatives
*   **Rejected Minimal Lifecycle (No Draining States)**: Discarded because immediate transition to `FAILED` or `CANCELLED` leaves active tasks running in dead workflows, causing state corruption and inconsistent observability.
*   **Rejected Generic `TERMINATING` State**: Discarded because merging failure and cancellation into a single state hides operational intent in secondary metadata and blurs API visibility.
*   **Rejected Purely Derived Lifecycle**: Discarded because external cancellation intent cannot be inferred from child task states alone, and dynamic aggregation under concurrency introduces race hazards.
*   **Rejected Dedicated `RECOVERING` State**: Discarded because recovery is an engine operational process, not a business workflow lifecycle condition.
*   **Rejected Immediate Terminalization on Failure**: Discarded to preserve the terminal task convergence invariant.

---

## 21. Decision Evolution
*   *2026-09-05*: Initial approval of the 7-state directional drain state machine, establishing eager initialization gating, fail-fast scheduling revocation, best-effort active work cancellation, terminal task convergence, and first-committed direction rules.

---

## 22. Common Misconceptions
*   *Misconception*: "A workflow fails immediately when an individual task attempt fails."
    *   *Correction*: Task retries are managed by ADR-007. The workflow state machine reacts only when a task reaches a definitive, permanent task-level failure outcome.
*   *Misconception*: "Best-effort cancellation guarantees that remote workers stop immediately."
    *   *Correction*: Network boundaries prevent instantaneous physical guarantees. ADR-006 guarantees logical settlement within the engine, while physical termination is pursued on a best-effort basis via ADR-008.
*   *Misconception*: "Leaf node completion is sufficient to declare workflow success."
    *   *Correction*: In disconnected DAGs or workflows with internal failures, leaf inspection alone is insufficient. Success requires 100% of declared tasks to reach terminal success.
*   *Misconception*: "A cancelled workflow means all tasks were cancelled."
    *   *Correction*: A cancelled workflow may contain tasks that succeeded prior to cancellation, tasks that failed, and unstarted tasks resolved as non-executable.

---

## 23. Open Questions
*   *Open Boundary A: Pre-Creation vs. Post-Creation Initialization Failure*: The exact criteria separating API-level start rejection (before durable `WorkflowExecution` creation) from an `INITIALIZING -> FAILED` transition will be refined in conjunction with data-flow ([ADR-010](00-architecture-decision-register.md#L209)), persistence ([ADR-011](00-architecture-decision-register.md#L210)), and API layer ([ADR-015](00-architecture-decision-register.md#L214)) decisions.
*   *Open Boundary B: Terminal Non-Executable Task State Label*: The concrete enum name chosen to represent tasks that will never execute due to workflow-level failure or cancellation (e.g., `BLOCKED`, `UPSTREAM_FAILED`, `SKIPPED`, or `CANCELLED`) is deferred to [ADR-007](00-architecture-decision-register.md#L202).

---

## 24. Interview Discussion

### Why use a 7-state model instead of a minimal RUNNING / SUCCEEDED / FAILED / CANCELLED model?
A minimal model cannot maintain the terminal cleanliness invariant. If a workflow transitions directly from `RUNNING` to `FAILED` when a task fails, sibling tasks are still executing on remote workers. Those workers will eventually report back to a closed workflow, creating race conditions, unhandled exceptions, and auditing anomalies. The intermediate `FAILING` and `CANCELLING` states provide a clear, durable draining window where new scheduling is halted, active tasks are instructed to stop, and all child entities logically settle before the execution is sealed.

### Why separate FAILING and CANCELLING instead of using a single TERMINATING state?
While both states perform draining, their causes, authorization semantics, and terminal destinations differ fundamentally. `CANCELLING` is triggered by an authenticated administrative command and leads exclusively to `CANCELLED`. `FAILING` is triggered by internal execution faults and leads exclusively to `FAILED`. Separate states provide immediate, unambiguous visibility for API callers without requiring secondary metadata parsing, while keeping recovery and transition logic explicit.

### Why is the schedulability predicate restricted strictly to RUNNING?
The scheduler's responsibility is to progress ready work. Once an external operator cancels a workflow or a fatal task failure occurs, overall workflow success is impossible. Permitting the scheduler to dispatch new tasks in `FAILING` or `CANCELLING` wastes worker compute and produces discarded outputs. Schedulability must be revoked immediately upon entering any draining state.

### What happens if an external cancellation command races with the final task completing successfully?
State machine precedence is governed by single-winner atomic transitions. If the task completion commits the transition `RUNNING -> SUCCEEDED` first, the workflow is immutably terminal; the cancellation request observes a terminal state and performs no mutation. If the cancellation commits `RUNNING -> CANCELLING` first, the workflow is locked into a cancellation trajectory; the task completion is recorded at the task level, but the workflow proceeds to `CANCELLED`.

### Why isn't RECOVERING an authoritative workflow lifecycle state?
Recovery is an operational process executed by the orchestrator upon restart, not an execution lifecycle condition. A workflow remains logically `RUNNING`, `FAILING`, or `CANCELLING` regardless of whether the orchestrator process crashed and rebooted. Introducing a `RECOVERING` state creates artificial state churn, complicates database indexing, and confuses API clients.

---

## 25. References
*   [ADR-001: Internal Workflow Specification](adr-001-internal-workflow-specification.md)
*   [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md)
*   [ADR-004: Workflow Validation Strategy](adr-004-workflow-validation-strategy.md)
*   [ADR-005: Workflow Task Scheduling & Dispatch Architecture](adr-005-workflow-task-scheduling-and-dispatch-architecture.md)
*   [Architecture Decision Register](00-architecture-decision-register.md)

---

## 26. Traceability
*   **Depends On**: [ADR-003](adr-003-canonical-workflow-graph-representation.md), [ADR-004](adr-004-workflow-validation-strategy.md), [ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md)
*   **Enables**: [ADR-007](00-architecture-decision-register.md#L202)
*   **Related To**: [ADR-008](00-architecture-decision-register.md#L203), [ADR-010](00-architecture-decision-register.md#L209), [ADR-011](00-architecture-decision-register.md#L210), [ADR-012](00-architecture-decision-register.md#L211), [ADR-013](00-architecture-decision-register.md#L212), [ADR-014](00-architecture-decision-register.md#L213), [ADR-015](00-architecture-decision-register.md#L214), [ADR-017](00-architecture-decision-register.md#L214)

---

## 27. Decision Validation Checklist
*   [x] Is the problem statement decoupled from specific database/broker technologies?
*   [x] Are the functional requirements (FRs) and non-functional requirements (NFRs) traced?
*   [x] Were at least two realistic candidate designs critically evaluated?
*   [x] Are the tradeoffs clear (what are we giving up for simplicity or correctness)?
*   [x] Does the design preserve all mapped system invariants?
*   [x] Does this decision avoid introducing tight coupling between modules?
*   [x] Are the potential failure modes mapped?
*   [N/A] Is there a clear explanation of how this design behaves during a graceful shutdown? *(Justification: Decoupled workflow state machine; graceful shutdown is an engine process concern owned by ADR-017).*
*   [x] Are the debugging strategies defined?
*   [x] Does the testing strategy explain how to simulate failures and recovery?
*   [x] Are the performance limits and resource footprints qualitatively identified?
*   [N/A] Is the future evolution path to HA or multi-tenant deployment explained? *(Justification: Single-node V1 focus; clustered state coordination is deferred to ADR-025).*
*   [x] Can this decision be defended during an SDE-2 engineering review?
