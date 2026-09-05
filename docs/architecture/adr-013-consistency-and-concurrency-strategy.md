# ADR-013 — Consistency & Concurrency Strategy

## 1. Purpose

This Architectural Decision Record (ADR) establishes the formal consistency model, concurrency control architecture, and single-winner transition discipline for the NexusFlow orchestration engine. It defines how the system guarantees correct, deterministic, single-winner lifecycle progression across concurrent worker callbacks, asynchronous scheduler loops, timers, recovery sweeps, and external API requests. 

Furthermore, this record formalizes the requirements for stale-write detection via opaque concurrency revisions, atomic durability consistency groups, multi-entity concurrency guards, and fail-closed unknown-commit resolution, while strictly preserving technology neutrality and deferring physical storage selection and concrete database schema design to [ADR-020](00-architecture-decision-register.md).

---

## 2. Context

NexusFlow executes complex, multi-step directed acyclic graph (DAG) workflows defined by [ADR-001](adr-001-internal-workflow-specification.md) and [ADR-003](adr-003-canonical-workflow-graph-representation.md). The runtime behavior of these workflows is governed by the state machines established in [ADR-006](adr-006-workflow-execution-state-machine.md) (`WorkflowExecution`) and [ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md) (`TaskExecution` and `ExecutionAttempt`). Worker interaction is coordinated under [ADR-008](adr-008-worker-coordination-and-liveness-model.md), task routing under [ADR-009](adr-009-task-routing-strategy.md), and parameter passing under [ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md). 

Under [ADR-011](adr-011-state-persistence-strategy.md), NexusFlow adopted a durable current-state persistence model anchored in authoritative durable storage and defined ten logical consistency groups. [ADR-012](adr-012-recovery-strategy.md) established the post-crash reconciliation strategy without relying on global recovery locks or maintenance barriers.

### The V1 Concurrency Reality
NexusFlow V1 assumes a single active orchestrator process. However, this single-process operational boundary does **not** eliminate concurrency. Extensive concurrency exists at the control plane due to:
1. **Concurrent Worker Callbacks:** Multiple distributed workers submitting execution heartbeats, start observations, and terminal execution results via concurrent asynchronous RPC/HTTP connections.
2. **Asynchronous Scheduler Loops:** Autonomous event loop tasks concurrently scanning for runnable tasks, evaluating dependency readiness, and attempting task dispatch.
3. **Concurrent Timers:** Independent asynchronous timers firing for attempt start deadlines, execution timeouts, and retry backoff delays.
4. **External API Invocations:** Concurrent user or system API requests submitting workflow cancellation, termination requests, or workflow/task status inspection.
5. **Recovery Reconciliation Sweeps:** Startup recovery reconciliation and any permitted targeted/defensive runtime reconciliation activity scanning durable state for overdue tasks or uncommitted transitions while normal scheduling continues.
6. **Task Routing Contention:** Multiple workers concurrently attempting to claim ownership of the same runnable task.

Without a rigorous, durable concurrency control strategy, concurrent operations across these actors can produce race conditions, including duplicate task ownership, lost results, corrupt attempt ordinal sequences, conflicting workflow terminalization, and zombie task executions.

---

## 3. Problem Statement

How does NexusFlow guarantee correct single-winner state progression when multiple control-plane actions race concurrently, ensuring that:
1. Two workers attempting ownership commit for the same runnable task result in exactly one authoritative attempt, without duplicate retry-budget consumption or duplicate ordinal allocation?
2. A worker execution result racing an execution timeout or user cancellation commits cleanly such that whichever transition commits first wins, with the loser fenced from publishing conflicting state?
3. A user cancellation racing definitive task failure direction resolves unambiguously without arbitrary priority, locking in the direction that commits first?
4. Scheduler readiness evaluations, recovery sweeps, and upstream task completions can evaluate the same downstream task without corrupting task inputs or duplicating state progression?
5. Multi-entity transitions (e.g., retry scheduling or workflow terminalization) cannot commit against stale cross-entity predicates?
6. Independent tasks within the same workflow can progress and settle concurrently without suffering from coarse, workflow-wide lock contention?
7. Unclear or interrupted commit outcomes (e.g., network disconnects during durable commit) are resolved safely without assuming failure or duplicating non-idempotent operations?

---

## 4. Requirements Covered

### Functional Concurrency Requirements
- **FR-CON-001:** Enforce single-winner state transitions across all concurrent control-plane mutations.
- **FR-CON-002:** Guarantee active attempt uniqueness (at most one non-terminal attempt per task).
- **FR-CON-003:** Guarantee unique, monotonically ordered committed Attempt ordinals per TaskExecution, with committed ordinals never reused.
- **FR-CON-004:** Protect cross-entity dependencies so that dependent state mutations commit only if prerequisite states remain valid at the commit boundary.
- **FR-CON-005:** Ensure task output payloads are committed atomically with terminal success transitions.
- **FR-CON-006:** Fence stale or superseded execution attempts from mutating task states or publishing outputs.

### Non-Functional Concurrency Requirements
- **NFR-COR-001 (Correctness Authority):** Concurrency correctness must be guaranteed exclusively by authoritative durable storage, completely independent of in-memory locks or transport ordering.
- **NFR-SCA-001 (Parallel Task Scalability):** Concurrency control must permit independent DAG branches to progress without mandatory workflow-wide serialization.
- **NFR-REL-001 (Crash Resilience):** State transitions must commit atomically within short-lived persistence transactions, holding zero network I/O inside transaction boundaries.
- **NFR-REL-002 (Fail-Closed Recovery):** Unknown commit outcomes must be reconciled through authoritative state inspection before retrying, preventing state duplication.

---

## 5. Constraints

1. **V1 Single-Orchestrator Scope:** V1 assumes a single active orchestrator process; multi-orchestrator active-active clustering is deferred to [ADR-025](00-architecture-decision-register.md).
2. **Zero Storage Technology Coupling:** ADR-013 must remain technology-neutral. It must not mandate SQL syntax, database vendor selections, physical isolation level keywords, or table schema implementations (owned by [ADR-020](00-architecture-decision-register.md)).
3. **No External Distributed Locks:** System correctness in V1 must not depend on an external distributed lock service (e.g., Redis Redlock, ZooKeeper, etcd).
4. **State Machine Invariant Preservation:** Concurrency control must strictly uphold the lifecycle transitions and invariants defined in [ADR-006](adr-006-workflow-execution-state-machine.md) and [ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md).
5. **Persistence Group Alignment:** Every concurrency mechanism must align with the ten logical consistency groups established in [ADR-011](adr-011-state-persistence-strategy.md).

---

## 6. Goals

- Define a durable, single-winner concurrency control strategy based on optimistic concurrency control (OCC), semantic state predicates, and opaque revision tokens.
- Establish the exact semantic linearization point of authoritative state transitions.
- Define multi-entity concurrency guards ensuring dependent transitions cannot commit against stale observations.
- Formalize single-winner arbitration across all competing control-plane lifecycle races.
- Ensure independent tasks in parallel DAG branches execute and transition without root workflow contention.
- Define a fail-closed reconciliation discipline for unknown commit outcomes.
- Provide a clear conceptual foundation that extends naturally to future multi-orchestrator high availability ([ADR-025](00-architecture-decision-register.md)).

---

## 7. Non-Goals

- Selecting a physical database engine, schema definition, SQL dialect, or storage engine isolation level (owned by [ADR-020](00-architecture-decision-register.md)).
- Defining external HTTP status codes, API headers, or REST idempotency key representations (owned by [ADR-015](00-architecture-decision-register.md)).
- Defining formal operational error taxonomies or string error codes (owned by [ADR-018](00-architecture-decision-register.md)).
- Specifying exact numerical retry counts, backoff multipliers, or jitter algorithms for OCC conflicts (owned by [ADR-023](00-architecture-decision-register.md)).
- Designing multi-orchestrator leader election, distributed consensus, or cluster fencing (owned by [ADR-025](00-architecture-decision-register.md)).
- Implementing business activity rollback or distributed transaction compensation (out of scope for V1).

---

## 8. Candidate Solutions

### Candidate A: Single Global Engine Mutex
All control-plane operations within the orchestrator process acquire a single global asynchronous lock (`asyncio.Lock`) before evaluating state, reading the database, or committing mutations.
- *Pros:* Trivial mental model; eliminates all race conditions within the process.
- *Cons:* Destroys concurrency; completely serializes independent workflows and tasks; provides zero crash resilience (locks vanish on restart); incompatible with multi-orchestrator HA ([ADR-025](00-architecture-decision-register.md)).

### Candidate B: Per-Workflow In-Memory Locking
Maintain an in-memory lock table mapping `WorkflowExecutionId` to an asynchronous lock. All operations pertaining to a workflow (task dispatch, callbacks, timer events) acquire the workflow lock.
- *Pros:* Prevents races on the same workflow within a single process; isolates workflows from one another.
- *Cons:* Serializes parallel DAG tasks within the same workflow; locks are lost across process restarts and recovery; does not protect against external database modifications; incompatible with future multi-node clustering without distributed lock managers.

### Candidate C: Universal Pessimistic Storage Locking
All state transitions begin by acquiring pessimistic row-level or table-level locks within the database (e.g., locking the `WorkflowExecution` row for the duration of every task update).
- *Pros:* Strongly prevents concurrent updates at the database level.
- *Cons:* High lock contention on workflow root entities; long-lived database connections; high risk of deadlocks across complex DAG evaluations; poor throughput on wide fan-out/fan-in graphs; tightly couples business flow duration to storage connection pools.

### Candidate D: Pure State-Predicate Conditional Updates
Transitions rely entirely on conditional state matching (e.g., mutating state only if current state equals expected state) without revision tokens or multi-entity guards.
- *Pros:* Highly concurrent; avoids explicit lock management.
- *Cons:* Cannot detect metadata-only races where lifecycle state remains unchanged; vulnerable to ABA problems during rapid retries; lacks protection for cross-entity predicates (e.g., verifying workflow state while updating task state).

### Candidate E (Selected): Durable Optimistic Single-Winner Concurrency with Opaque Revisions and Multi-Entity Guards
Orchestration state is governed by Optimistic Concurrency Control (OCC) anchored in authoritative durable storage. Every mutable entity exposes an opaque concurrency revision. Authoritative mutations commit only if expected lifecycle states, revision tokens, and required semantic predicates hold at the commit boundary. Multi-entity transitions commit atomically within short-lived persistence transactions. In-memory synchronization is an optional local performance optimization.

---

## 9. Detailed Evaluation

| Evaluation Criterion | Candidate A (Global Mutex) | Candidate B (Workflow Lock) | Candidate C (Pessimistic Storage) | Candidate D (Pure State Predicates) | Candidate E (Durable OCC - Selected) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Durable Authority** | None (in-memory) | None (in-memory) | Storage-enforced | Storage-enforced | **Storage-enforced** |
| **Crash & Recovery Safety** | Broken on restart | Broken on restart | Resilient | Resilient | **Restart-safe under ADR-011/012** |
| **Parallel DAG Scalability** | Zero | Serialized per workflow | Severely restricted | High | **High (task-level independence)** |
| **Stale Metadata Detection** | N/A | High (in-process only) | High | None (state enum only) | **High (opaque revision)** |
| **Multi-Entity Guarding** | Memory-only | Memory-only | High (via shared locks) | None (prone to torn checks) | **Protected at commit boundary** |
| **Deadlock Vulnerability** | None | Low | High | None | **Low (short bounded transactions)** |
| **No Network in Transactions** | Enforced | Enforced | Frequently violated | Enforced | **Strictly enforced** |
| **HA Preparedness (ADR-025)** | Impossible | Impossible | Moderate (high contention) | High | **Strong foundation** |

---

## 10. Decision

NexusFlow adopts **Durable Optimistic Single-Winner Concurrency** as its core consistency and concurrency architecture for V1.

### 10.1 Core Architectural Principles
1. **Durable Storage as the Sole Correctness Authority:** Concurrency correctness is guaranteed exclusively by authoritative durable state. In-memory locks, queue ordering, transport protocols, and worker claims have zero standing as correctness authorities.
2. **Single-Winner Transition Rule:** When multiple operations attempt conflicting or identical authoritative transitions, exactly one valid mutation establishes durable authority. Competing operations observe precondition or revision failure, reload authoritative state, and resolve idempotently or re-evaluate.
3. **Dual-Layer Concurrency Guard:** Every authoritative state mutation requires that:
   - The entity’s **current lifecycle state** matches the expected state.
   - The entity’s **opaque concurrency revision** matches the observed revision.
   - All **semantic domain predicates** hold at the durable commit boundary.
4. **Semantic Linearization Point:** The successful durable commit of an authoritative persistence transaction is the semantic linearization point of that state transition. Speculative operations prior to commit carry no authority; after commit, all observers must respect the outcome.
5. **Control-Plane Contention Isolation:** A lost optimistic concurrency race, revision conflict, or transaction serialization conflict is strictly an internal control-plane coordination event. It must never trigger a task business execution failure or mark a workflow as failed.
6. **Task-Level Concurrency Independence:** Parallel tasks within the same workflow progress, transition, and commit independently. Normal task state progression does not acquire locks on or mutate the root `WorkflowExecution` entity.
7. **Strict Transaction Scope:** Authoritative persistence transactions are short-lived state mutation blocks covering only the smallest correctness-critical consistency group. Holding persistence transactions open across network calls, worker RPCs, API waits, sleep timers, or broker publishing is architecturally prohibited.
8. **Fail-Closed Unknown Commit Resolution:** If an orchestrator client or connection drops before receiving a commit acknowledgement, the control plane must re-read authoritative durable state before retrying, never assuming failure.

---

## 11. Decision Rationale

### 11.1 Why Optimistic Concurrency Control (OCC)?
Workflows are directed acyclic graphs where the vast majority of tasks execute along independent parallel branches. True concurrent contention is exceptional, occurring primarily during simultaneous worker claims for the same task, race conditions between terminal results and timeouts, or user cancellation racing task completion. 

Adopting OCC allows independent tasks to execute and commit without acquiring coarse, blocking locks on parent workflow aggregates. It avoids mandatory workflow-wide serialization and reduces avoidable lock contention during distributed execution.

### 11.2 Why Opaque Concurrency Revisions?
Semantic state predicates (e.g., verifying `state == RUNNABLE`) protect lifecycle enum transitions. However, mutable entities also undergo non-lifecycle metadata updates (e.g., worker session heartbeat observations, diagnostic metadata, permitted cancellation context). A revision token ensures that any concurrent modification—even one that does not alter the primary state enum—is immediately detected, preventing silent lost updates. 

The revision token is specified as **opaque** to decouple control-plane stale-write detection from physical database representations (e.g., integer version, timestamp, cryptographic hash), which is owned by [ADR-020](00-architecture-decision-register.md).

### 11.3 Multi-Entity Concurrency Guards
Certain state transitions depend on the state of another mutable entity. For example, transitioning a task to `RUNNABLE` or scheduling a retry requires that the parent `WorkflowExecution` is currently `RUNNING`. If a user cancels the workflow concurrently, a naive read-then-write would allow the task mutation to commit against a stale observation.

ADR-013 establishes the **Multi-Entity Concurrency Guard Rule**:
> *If an authoritative mutation depends on another mutable entity's state, that dependency must participate in the concurrency guard strongly enough that a conflicting mutation cannot invalidate the predicate before commit.*

This rule guarantees that if `WorkflowExecution` transitions to `CANCELLING`, any concurrent task attempt to transition to `RUNNABLE` or `RETRY_WAIT` will fail its multi-entity predicate and roll back.

---

## 12. Tradeoffs

| Advantage | Tradeoff / Mitigation |
| :--- | :--- |
| **High Parallel Throughput:** Independent tasks commit without root aggregate contention. | **Contention Retry Overhead:** Highly contended entities (e.g., duplicate callbacks) require reload and retry. *Mitigation:* Bounded retries with exponential backoff. |
| **Crash & Recovery Resilience:** Authoritative durable state guarantees correctness without in-memory synchronization. | **Storage Engine Demands:** Requires persistence storage supporting atomic multi-entity mutations and conditional updates. |
| **Technology Neutrality:** Architecture does not depend on specific SQL or NoSQL database features. | **Abstraction Rigor:** Demands strict semantic discipline in application code rather than relying on vendor-specific locks. |
| **Clean HA Foundation:** Naturally supports multi-orchestrator scale-out in ADR-025. | **No Distributed Magic:** Still requires fail-closed reconciliation for network interruptions and dropped commits. |

---

## 13. Consequences

### 13.1 Positive Consequences
- **Elimination of Split-Brain Ownership:** Two workers claiming the same task cleanly arbitrate at the persistence boundary; exactly one wins.
- **Robust Zombie Fencing:** Delayed or duplicate execution results from stale attempts are fenced at the durable commit boundary.
- **Unambiguous Direction Races:** Workflow cancellation, failure, and success races resolve deterministically based on which valid transition commits first.
- **Independent Task Scalability:** Widely branching DAGs execute without contention on the workflow root row.
- **Restart-Safe Coordination:** Background recovery sweeps execute concurrently with live schedulers without requiring maintenance modes or global locks.

### 13.2 Negative / Constraining Consequences
- **Application Complexity:** All control-plane mutation paths must implement re-read, re-evaluate, and idempotent resolution logic.
- **Zero Transactional Network Calls:** Orchestrator code cannot use distributed transactions (2PC) or hold database transactions open during worker RPC handshakes.
- **Strict Storage Capabilities:** The storage engine chosen in [ADR-020](00-architecture-decision-register.md) must strictly support atomic multi-entity mutation and conditional write rejection.

---

## 14. Failure Modes & Concurrency Arbitrations

The following matrix defines the authoritative starting facts, competing operations, allowed single-winner outcomes, loser behaviors, and semantic guards across all recognized NexusFlow control-plane races.

| # | Race Condition | Competing Operations | Allowed Winner | Loser Behavior | Semantic Concurrency Guard | Owning ADR |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | **Concurrent Task Claim** | Worker 1 vs. Worker 2 attempting Ownership Commit | First to commit Group 2 | Fails conditional predicate (`state == RUNNABLE`); transaction rolls back; returns claim rejection to worker; no authoritative Attempt is committed and no retry-budget slot is consumed by the losing operation | Group 2 atomic transaction: conditional update on `TaskExecution.state == RUNNABLE` + opaque revision check | ADR-013 / ADR-008 |
| **2** | **Unknown Ownership Commit** | Client connection drops during Group 2 commit | Undetermined | Control plane re-reads authoritative state using stable attempt identity before retrying; fails closed if storage unreachable | Idempotent state re-query by `(TaskExecutionId, AttemptId)`; never assumes failure | ADR-013 / ADR-011 |
| **3** | **Attempt Ordinal Race** | Two attempt creations concurrently allocating ordinal | First to commit | Second fails uniqueness invariant within `TaskExecution`; rolls back and re-evaluates | Committed ordinal uniqueness invariant per `TaskExecution` (committed ordinals never reused) | ADR-013 / ADR-007 |
| **4** | **Duplicate Attempt Creation** | Duplicate dispatch or recovery sweep creating attempt | First commit | Second observes active attempt already exists; rolls back and no-ops | Active attempt uniqueness invariant (`CLAIMED` or `RUNNING`) | ADR-013 / ADR-008 |
| **5** | **Start vs. Start Timeout** | Worker reports start vs. start deadline timeout fires | First to commit Group 3 | If start wins, timeout no-ops. If timeout wins, attempt becomes `FAILED`; late start report fails predicate and is fenced | Conditional update on `ExecutionAttempt.state == CLAIMED` + revision check | ADR-013 / ADR-008 |
| **6** | **Start vs. Cancellation** | Worker reports start vs. workflow cancellation drain | First commit | If start wins, attempt is `RUNNING` and drained under ADR-007. If cancel wins, attempt becomes `CANCELLED`; late start fenced | Conditional update on `ExecutionAttempt.state == CLAIMED` | ADR-013 / ADR-006 |
| **7** | **Result vs. Execution Timeout** | Worker submits success result vs. execution timer fires | First commit | If result wins, Group 4 commits success and output; timer no-ops. If timer wins, attempt fails; late result fails predicate and is fenced | Conditional update on `ExecutionAttempt.state == RUNNING` + revision check | ADR-013 / ADR-007 |
| **8** | **Result vs. Cancellation** | Worker submits result vs. workflow cancellation drain | First commit | If result wins, Group 4 commits task success and output; workflow continues drain. If cancel wins, attempt is `CANCELLED`; late result fenced | Conditional update on `ExecutionAttempt.state == RUNNING` | ADR-013 / ADR-006 |
| **9** | **Worker Loss vs. Late Result** | Heartbeat reaper fails attempt vs. worker submits result | First commit | If result wins, settles success with output. If reaper wins, attempt fails; late result rejected | Conditional update on `ExecutionAttempt.state == RUNNING` | ADR-013 / ADR-008 |
| **10** | **Duplicate Identical Results** | Duplicate callback for same successful attempt | First to commit Group 4 | Second re-reads state, observes `SUCCEEDED` with identical output; resolves as idempotent success | Group 4 atomic commit + idempotent post-commit state re-read | ADR-013 / ADR-010 |
| **11** | **Conflicting Duplicate Results** | Duplicate callback with differing output payload | First to commit Group 4 | Second observes `SUCCEEDED` with differing output; rejects write, logs system diagnostic anomaly | Committed output immutability | ADR-013 / ADR-010 |
| **12** | **Stale Attempt Result** | Delayed result from failed Attempt 1 arrives while Attempt 2 is `RUNNING` | Attempt 2 remains active | Attempt 1 fails active attempt validation; cannot mutate task, write output, or unblock dependencies | Predicate: callback Attempt must be the authoritative active attempt for `TaskExecution` | ADR-013 / ADR-007 |
| **13** | **Attempt Failure vs. Workflow Cancel** | Active attempt fails while workflow cancels | Single-winner arbitration | Whichever commits first governs retryability; both settle attempt as `FAILED` | Multi-entity guard on `WorkflowExecution.state == RUNNING` | ADR-013 / ADR-006 |
| **14** | **Retry vs. Cancellation** | Retry scheduling (Group 5) vs. workflow cancel (Group 8) | First commit | If cancel wins, Group 5 fails multi-entity predicate; active attempt settles `FAILED`; no retry scheduled; task settles `FAILED` | Group 5 requires `WorkflowExecution.state == RUNNING` at commit boundary | ADR-013 / ADR-006 |
| **15** | **Retry vs. Failure Direction** | Retry scheduling (Group 5) vs. workflow failure (Group 7) | First commit | If failure direction wins, Group 5 fails predicate; task settles `FAILED` with no retry | Group 5 requires `WorkflowExecution.state == RUNNING` at commit boundary | ADR-013 / ADR-006 |
| **16** | **RETRY_WAIT -> RUNNABLE vs. Cancel** | Retry timer expires vs. workflow cancel commits | First commit | If cancel wins, transition rejected; task in `RETRY_WAIT` is unstarted and transitions to `CANCELLED` under drain | Predicate: `WorkflowExecution.state == RUNNING` | ADR-013 / ADR-006 |
| **17** | **PENDING -> RUNNABLE vs. Cancel** | Readiness progression vs. workflow cancel commits | First commit | If cancel wins, readiness rejected; unstarted task transitions to `CANCELLED` under drain | Predicate: `WorkflowExecution.state == RUNNING` | ADR-013 / ADR-006 |
| **18** | **Duplicate Readiness Evaluators** | Scheduler loop vs. recovery sweep evaluating same task | First commit | Second observes `TaskExecution.state == RUNNABLE`; rolls back and gracefully no-ops | Conditional update on `TaskExecution.state == PENDING` + revision check | ADR-013 / ADR-005 |
| **19** | **Task Input Materialization Race** | Two threads compute and write task input in Group 1 | First commit | Upstream outputs immutable; inputs identical; winner writes input; loser discards speculative write | Group 1 atomic commit: input write + `PENDING -> RUNNABLE` | ADR-013 / ADR-010 |
| **20** | **Workflow Failure vs. Cancel** | Definitive task failure (Group 7) vs. user cancel (Group 8) | First commit | Whichever commits first establishes direction; later transition rejected; losers re-read state | Conditional update on `WorkflowExecution.state == RUNNING` + revision check | ADR-013 / ADR-006 |
| **21** | **Workflow Success vs. Cancel** | All tasks succeed (Group 10) vs. user cancel (Group 8) | First commit | If success wins, cancel rejected (workflow terminal). If cancel wins, final task output saved, but workflow drains to `CANCELLED` | Conditional update on `WorkflowExecution.state == RUNNING` + revision check | ADR-013 / ADR-006 |
| **22** | **Stale Success Evaluation** | Success evaluation races late active task failure | Task failure | Success commit evaluates all tasks terminal and `SUCCEEDED`; if any task fails, success predicate is violated | Multi-entity predicate: all expected tasks must be `SUCCEEDED` at commit boundary | ADR-013 / ADR-006 |
| **23** | **FAILING Terminalization vs. Task Settlement** | Workflow `FAILING -> FAILED` races late active attempt settlement | Serialization point | Terminalization requires that zero non-terminal tasks exist; if any task active, terminalization rejected | Set-predicate serialization point check: all expected tasks must be terminal | ADR-013 / ADR-006 |
| **24** | **CANCELLING Terminalization vs. Task Settlement** | Workflow `CANCELLING -> CANCELLED` races late active attempt settlement | Serialization point | Terminalization requires that zero non-terminal tasks exist; if any task active, terminalization rejected | Set-predicate serialization point check: all expected tasks must be terminal | ADR-013 / ADR-006 |
| **25** | **Duplicate Cancellation Requests** | Two concurrent user cancellation requests | First commit | Advances `RUNNING -> CANCELLING`; second observes `CANCELLING` and resolves as idempotent success | Idempotent transition resolution | ADR-013 / ADR-015 |
| **26** | **Duplicate Timer Firings** | Same timeout or retry backoff timer fires twice | First commit | First advances state; second matches 0 records on expected non-terminal state; harmless no-op | Conditional predicate on expected state (`CLAIMED`, `RUNNING`, or `RETRY_WAIT`) | ADR-013 / ADR-007 |
| **27** | **Recovery vs. Scheduler Readiness** | Startup recovery sweep races normal scheduler loop | First commit | First commits readiness or dispatch; second observes updated state/revision and no-ops | Identical durable OCC rules applied across recovery and runtime | ADR-013 / ADR-012 |
| **28** | **Recovery vs. Result Callback** | Recovery sweep evaluates overdue task while worker submits result | First commit | Whichever commits first wins; loser observes state change and re-evaluates | Identical durable OCC rules applied across recovery and callbacks | ADR-013 / ADR-012 |
| **29** | **Recovery vs. Cancellation** | Recovery reconciliation races external cancellation | First commit | Handled via standard single-winner transitions; direction established by committed winner | Multi-entity guards and state predicates | ADR-013 / ADR-012 |
| **30** | **Unknown Result Commit Outcome** | Disconnect during Group 4 result commit | Undetermined | Control plane inspects attempt, task, and committed output before retrying; resolves idempotently | Authoritative re-query / idempotent inspection | ADR-013 / ADR-010 |
| **31** | **Unknown Workflow Transition Outcome** | Disconnect during root workflow direction change | Undetermined | Re-read `WorkflowExecution.state` from storage; resume based on durable truth | Authoritative state inspection before retry | ADR-013 / ADR-006 |
| **32** | **OCC Revision Conflict** | Concurrent updates to same entity | Winner commits | Loser detects revision mismatch; reloads authoritative state; re-evaluates if still valid | Opaque concurrency revision check | ADR-013 |
| **33** | **Storage Outage Mid-Transition** | Persistence storage unavailable during transaction | Fail closed | Transaction rolls back; no in-memory mutation; error bubbled as operational retry | Fail-closed persistence boundary | ADR-013 / ADR-011 |
| **34** | **Process Crash Before Commit** | Orchestrator crashes before transaction commits | Uncommitted | Uncommitted operations rolled back by storage; post-restart recovery rediscovers durable state | Atomic persistence transaction boundary | ADR-013 / ADR-012 |
| **35** | **Process Crash Immediately After Commit** | Orchestrator crashes after commit but before dispatch RPC | Committed | Commit is durable; restart recovery rediscovers `RUNNING`/`CLAIMED` or `RUNNABLE` state and resumes | Durable commit as linearization point | ADR-013 / ADR-012 |
| **36** | **Parallel Task Updates** | Task A and Task B in same workflow progress concurrently | Both commit | Both commit independently in separate transactions without workflow root contention | Entity-level concurrency boundaries | ADR-013 |
| **37** | **Fan-In Readiness Evaluation** | Upstream Tasks A & B complete $\to$ evaluate Downstream D | Single-winner | First scheduler pass commits `D: PENDING -> RUNNABLE`; second pass observes `RUNNABLE` and no-ops | Conditional update on `TaskExecution D.state == PENDING` | ADR-013 / ADR-005 |
| **38** | **Fan-Out Readiness Evaluation** | Upstream Task A completes, unlocking Tasks D1 and D2 | Independent | Both transition independently in separate transactions; contention on D1 does not impact D2 | Independent entity-level OCC transactions | ADR-013 / ADR-005 |

---

## 15. Consistency Group Concurrency Specifications

The ten logical consistency groups established in [ADR-011](adr-011-state-persistence-strategy.md) map to the following atomic concurrency specifications:

### 15.1 Group 1 — Task Input Materialization & Readiness
- **Participating Facts:** `TaskExecution.state: PENDING -> RUNNABLE`, materialized task input payload, opaque revision advancement.
- **Predicates:** `TaskExecution.state == PENDING`, all direct upstream dependencies `SUCCEEDED`, `WorkflowExecution.state == RUNNING`.
- **Concurrency Rule:** Single-winner. Multiple evaluators compute identical input from immutable upstream outputs; only the winning transaction writes input and commits `RUNNABLE`.

### 15.2 Group 2 — Task Ownership Commit
- **Participating Facts:** `TaskExecution.state: RUNNABLE -> RUNNING`, `ExecutionAttempt` created in `CLAIMED` with `WorkerSessionId`, attempt ordinal assigned, revision advancement.
- **Predicates:** `TaskExecution.state == RUNNABLE`, `WorkflowExecution.state == RUNNING`, zero authoritative active attempts exist.
- **Worker Liveness Boundary:** Best-known worker liveness is revalidated from the ephemeral Worker Registry immediately prior to commit. Liveness is not part of the durable transaction. If the worker fails immediately post-commit, start deadlines and recovery govern.

### 15.3 Group 3 — Execution Start Observation
- **Participating Facts:** `ExecutionAttempt.state: CLAIMED -> RUNNING`, revision advancement.
- **Predicates:** `ExecutionAttempt.state == CLAIMED`, execution start deadline unexpired.
- **Concurrency Rule:** Start observation races start timeout. First commit wins. Winning start observation permanently deactivates start-deadline enforcement.

### 15.4 Group 4 — Task Success & Output Commitment
- **Participating Facts:** `ExecutionAttempt.state: RUNNING -> SUCCEEDED`, `TaskExecution.state: RUNNING -> SUCCEEDED`, authoritative task output payload, revision advancement.
- **Predicates:** Callback attempt matches the authoritative active attempt, `ExecutionAttempt.state == RUNNING`, `TaskExecution.state == RUNNING`.
- **Workflow State Independence:** Active task result settlement does **not** require `WorkflowExecution.state == RUNNING`. Tasks completing during `FAILING` or `CANCELLING` workflow drain commit success and output normally.

### 15.5 Group 5 — Retry Scheduling
- **Participating Facts:** `ExecutionAttempt.state: RUNNING -> FAILED`, `TaskExecution.state: RUNNING -> RETRY_WAIT`, scheduled retry timestamp, revision advancement.
- **Predicates:** Active attempt failure, retry budget remains, retry permitted by policy, `WorkflowExecution.state == RUNNING`.
- **Drain Race Rule:** Group 5 requires that `WorkflowExecution.state == RUNNING` at the commit boundary. If workflow cancellation commits first, Group 5 fails; the attempt settles as `FAILED`, no retry is scheduled, and the task settles as `FAILED`.

### 15.6 Group 6 — Definitive Task Failure
- **Participating Facts:** `ExecutionAttempt.state: RUNNING -> FAILED`, `TaskExecution.state: RUNNING -> FAILED`, revision advancement.
- **Predicates:** Active attempt failure, no retry scheduled (due to budget exhaustion, non-retryable failure, or workflow no longer `RUNNING`).

### 15.7 Group 7 — Workflow Failure Direction
- **Participating Facts:** `WorkflowExecution.state: RUNNING -> FAILING`, revision advancement.
- **Predicates:** `WorkflowExecution.state == RUNNING`, at least one definitive `TaskExecution` failure exists.
- **Decoupled Atomicity:** Task failure (Group 6) and Workflow failure direction (Group 7) are separate consistency groups. A transient crash between Group 6 and Group 7 leaves a valid intermediate state (`TaskExecution FAILED`, `WorkflowExecution RUNNING`), which is reconciled safely by ADR-012 recovery.

### 15.8 Group 8 — Workflow Cancellation Direction
- **Participating Facts:** `WorkflowExecution.state: RUNNING -> CANCELLING` (or `INITIALIZING -> CANCELLING`), revision advancement.
- **Predicates:** `WorkflowExecution.state IN (INITIALIZING, RUNNING)`.
- **Single-Winner Arbitration:** Races with Group 7 and Group 10. Whichever valid transition commits first locks in workflow direction. Duplicate cancellation requests resolve idempotently.

### 15.9 Group 9 — Initialization Terminal Failure
- **Participating Facts:** `WorkflowExecution.state: INITIALIZING -> FAILED`, failure diagnostic payload, revision advancement.
- **Predicates:** `WorkflowExecution.state == INITIALIZING`, definitive unrecoverable semantic initialization failure under ADR-006.

### 15.10 Group 10 — Workflow Success & Output Commitment
- **Participating Facts:** `WorkflowExecution.state: RUNNING -> SUCCEEDED`, authoritative workflow output payload, revision advancement.
- **Predicates:** `WorkflowExecution.state == RUNNING`, complete expected `TaskExecution` membership exists, all required tasks are `SUCCEEDED`, workflow output successfully resolved.

---

## 16. Workflow Terminalization Set Predicates

Transitions to terminal workflow states (`FAILING -> FAILED` and `CANCELLING -> CANCELLED`) govern the final shutdown of execution.

### 16.1 The Set-Predicate Race
If terminalization logic simply evaluates an in-memory counter or stale snapshot, a late-settling active task could be omitted, causing the workflow to become terminal while an attempt is still executing or settling output.

### 16.2 The Serialization Point Requirement
Authoritative terminalization must establish a serialization point at the commit boundary verifying that:
1. `WorkflowExecution` is currently in the expected draining state (`FAILING` or `CANCELLING`).
2. The complete expected `TaskExecution` membership exists for the workflow.
3. Every expected `TaskExecution` is currently in a terminal state (`SUCCEEDED`, `FAILED`, or `CANCELLED`).

Because terminal task states are permanently immutable once committed, once this set predicate is satisfied at the serialization point, no subsequent task mutation can invalidate it. Derived counters (`remaining_tasks`) remain non-authoritative performance caches and must never be the sole authority governing terminalization.

---

## 17. Unknown Commit Outcomes & Fail-Closed Resolution

In distributed systems, a network disconnect or client timeout during transaction commit leaves the caller in an ambiguous state: the transaction may have committed successfully on the storage node, or it may have rolled back.

### 17.1 The Golden Rule: Never Assume Failure
The orchestrator must **never** assume that an unknown commit outcome represents a failure, and must **never** blindly repeat a non-idempotent mutation.

### 17.2 Resolution Discipline
```
[ Commit Response Interrupted / Ambiguous ]
                    │
                    ▼
       [ Re-Query Authoritative Storage ]
       (Using Stable Correlation / AttemptId)
                    │
   ┌────────────────┼────────────────┐
   ▼                ▼                ▼
[ Mutation Found ] [ State Unchanged ] [ Conflicting State ]
   │                │                │
(Commit Won)     (Commit Lost)    (Competing Winner)
Resume Post-     Re-evaluate &    Respect Committed
Commit Actions   Safely Retry     Winner / Fence Loser
```

1. **Re-Read Authoritative State:** Query the database using the unique entity identity (`TaskExecutionId`, `AttemptId`, or `WorkflowExecutionId`).
2. **Classify Outcome:**
   - **Mutation Present:** The commit succeeded. Resume normal post-commit processing (e.g., dispatch notifications, worker acknowledgements).
   - **Original State Unchanged:** The transaction rolled back or never reached the storage engine. Re-evaluate preconditions and safely retry.
   - **Conflicting State Present:** Another operation won the race. Respect the committed state and abort the retry.
   - **Storage Unreachable:** Fail closed. Do not proceed based on memory assumptions; await storage recovery.

---

## 18. Technology Selection & ADR-020 Boundary

ADR-013 intentionally defines **semantic concurrency requirements** and refrains from selecting concrete database implementations.

### Capabilities Required of the Physical Persistence Layer (ADR-020):
1. **Atomic Multi-Entity Mutation:** Capability to commit updates across multiple distinct entities within a single atomic boundary.
2. **Conditional State Mutation / Stale-Write Rejection:** Capability to condition mutations on expected state and revision tokens, rejecting writes when conditions fail.
3. **Uniqueness and Integrity Guarantees:** Capability to enforce uniqueness for attempt ordinals and active attempts per task execution.
4. **Consistent Evaluation of Cross-Entity Predicates:** Capability to verify multi-entity predicates at the commit boundary.
5. **Selective Authoritative Reads:** Capability to perform targeted, consistent reads by primary and correlation identifiers.

*Concrete SQL syntax, database vendor selection, table schemas, physical isolation levels (e.g., Read Committed vs. Serializable), index structures, and physical constraint keywords belong strictly to [ADR-020](00-architecture-decision-register.md).*

---

## 19. External Boundaries & Decoupling

- **Message Broker Boundary:** NexusFlow does not require two-phase commit (2PC) between durable storage and message brokers. For pre-ownership dispatch messages, loss is harmless because the task remains `RUNNABLE` and is rediscovered by schedulers. For post-ownership notification loss, the task is `RUNNING` with attempt `CLAIMED`; ADR-008 start deadlines and ADR-012 recovery reconcile the lost message.
- **In-Memory Locks:** Local mutexes or `asyncio.Lock` primitives are classified as non-authoritative performance optimizations. The system must remain fully correct if all in-memory locks are omitted or cleared across restarts.
- **Distributed Locks:** A distributed lock service (e.g., Redis Redlock) is explicitly rejected as a correctness authority for V1, avoiding dual-authority split-brain risks.
- **Future HA (ADR-025):** By anchoring concurrency in durable conditional mutations rather than process memory, ADR-013 establishes a clean foundation for multi-orchestrator clustering. However, ADR-013 does not pre-solve HA: [ADR-025](00-architecture-decision-register.md) must define orchestrator leadership, epoch fencing, and partition tolerance.

---

## 20. Testing & Verification Strategy

All concurrency test suites are planned requirements and must be implemented as part of engine verification:
1. **Deterministic Race Injection Tests:** Harness using artificial yield points to intercept operations immediately prior to commit, injecting competing mutations (e.g., worker claim vs. worker claim; result vs. timeout; retry vs. cancellation).
2. **Dual-Worker Ownership Contention Suite:** Simulate concurrent claim requests for the same runnable task; verify exactly one authoritative Attempt commits, exactly one retry-budget Attempt is consumed, no duplicate committed ordinal exists, and the losing ownership operation gains no authority and receives a clean rejection.
3. **Terminal Race Arbitration Suite:** Concurrently fire worker success results and execution timeout timers; verify that whichever commits first wins, the loser is fenced, and outputs remain uncorrupted.
4. **Workflow Direction Race Suite:** Concurrently fire user cancellation and definitive task failure; verify that the committed direction holds and is never overwritten.
5. **Multi-Entity Predicate Invalidation Suite:** Invalidate parent workflow state while a task retry or readiness transition is in flight; verify the dependent task transaction rolls back cleanly.
6. **Set-Predicate Terminalization Suite:** Simulate late active task settlement during workflow terminalization; verify workflow cannot transition to `FAILED` or `CANCELLED` while any task remains active.
7. **Unknown-Commit Simulation Suite:** Drop network connections during commit transactions; verify the orchestrator queries authoritative state and reconciles correctly without duplicating non-idempotent operations.
8. **Parallel Branch Independence Suite:** Execute wide DAGs with hundreds of parallel tasks; verify tasks progress and commit without mutual contention or workflow root locking.

---

## 21. Operational Considerations

- **Contention Monitoring:** Control-plane metrics must track OCC revision conflict rates, commit retry counts, and unknown-commit reconciliation events. A sudden spike in conflict rates indicates pathological client behavior or excessive callback duplication.
- **Conflict Retries:** Internal OCC conflict retries must be strictly bounded with jittered exponential backoff (configured under [ADR-023](00-architecture-decision-register.md)) to prevent CPU spinning under contention.
- **Fail-Closed Alerting:** If an unknown commit outcome cannot be reconciled due to persistent storage unreachability, the orchestrator surfaces high-priority operational alerts and halts dependent workflows.

---

## 22. Rejected Alternatives

1. **Universal Distributed Locks (Redis / ZooKeeper):** Rejected because authoritative durable storage already provides single-winner commit capabilities. Adding an external lock manager introduces another points-of-failure cluster, network latency, and split-brain risks without improving correctness.
2. **Global Workflow-Wide Row Locking:** Rejected because locking the workflow aggregate during task execution serializes parallel DAG tasks, creating avoidable bottlenecks on wide workflows.
3. **Pure State Predicates Without Revision Tokens:** Rejected because metadata updates that do not advance lifecycle enums (e.g., heartbeat timestamps) remain vulnerable to lost updates without an opaque revision token.
4. **Transactional 2PC Between Database and Message Broker:** Rejected because distributed 2PC introduces extreme operational complexity and latency. NexusFlow relies on durable state as the source of truth, with recovery loops repairing lost messages.

---

## 23. Common Misconceptions

- **Misconception: A single orchestrator process eliminates concurrency.**
  *Reality:* Async task scheduling, distributed worker callbacks, timers, API calls, and recovery reconciliation sweeps run concurrently within that single process, creating critical race conditions that demand durable concurrency control.
- **Misconception: In-memory async locks are sufficient for single-process engines.**
  *Reality:* In-memory locks vanish upon process crash, do not protect across restarts, fail to reconcile external recovery sweeps, and prevent future HA scale-out.
- **Misconception: An OCC conflict represents an execution failure.**
  *Reality:* An OCC conflict is a transient control-plane contention event. It must be resolved by reloading state and retrying, and must never fail a business task.
- **Misconception: The orchestrator should assume a dropped commit failed.**
  *Reality:* A dropped commit may have succeeded on the storage node. Assuming failure leads to duplicate attempt creation and corrupted retry budgets. The engine must re-query durable truth.

---

## 24. Open Questions

- *None.* All concurrency boundaries, state machine races, multi-entity guards, set-predicate serialization requirements, and fail-closed reconciliation procedures are fully resolved.

---

## 25. References

- [ADR-001: Internal Workflow Specification](adr-001-internal-workflow-specification.md)
- [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md)
- [ADR-005: Workflow Task Scheduling & Dispatch Architecture](adr-005-workflow-task-scheduling-and-dispatch-architecture.md)
- [ADR-006: WorkflowExecution State Machine](adr-006-workflow-execution-state-machine.md)
- [ADR-007: TaskExecution Lifecycle & Attempt Model](adr-007-task-execution-lifecycle-and-attempt-model.md)
- [ADR-008: Worker Coordination & Liveness Model](adr-008-worker-coordination-and-liveness-model.md)
- [ADR-009: Task Routing Strategy](adr-009-task-routing-strategy.md)
- [ADR-010: Workflow Data Flow & Parameter Passing](adr-010-workflow-data-flow-and-parameter-passing.md)
- [ADR-011: State Persistence Strategy](adr-011-state-persistence-strategy.md)
- [ADR-012: Recovery Strategy](adr-012-recovery-strategy.md)
- [00-Architecture Decision Register](00-architecture-decision-register.md)

---

## 26. Traceability

| Requirement | Architectural Component | Section Reference |
| :--- | :--- | :--- |
| **FR-CON-001** | Single-Winner Transition Model | Section 10.1, 14 |
| **FR-CON-002** | Active Attempt Uniqueness Guard | Section 14 (#4), 15.2 |
| **FR-CON-003** | Monotonic Attempt Ordinal Allocation | Section 14 (#3), 15.2 |
| **FR-CON-004** | Multi-Entity Concurrency Guard | Section 11.3, 15.1, 15.5 |
| **FR-CON-005** | Atomic Group 4 Task Success & Output | Section 15.4 |
| **FR-CON-006** | Stale Attempt Result Fencing | Section 14 (#12) |
| **NFR-COR-001**| Durable Authority Supremacy | Section 10.1 |
| **NFR-SCA-001**| Independent Task Concurrency | Section 10.1 (#6) |
| **NFR-REL-001**| Bounded Persistence Transactions (No Network) | Section 10.1 (#7) |
| **NFR-REL-002**| Fail-Closed Unknown Commit Resolution | Section 17 |

---

## 27. Decision Validation Checklist

- [x] **Durable Optimistic Single-Winner Concurrency selected as core model.**
- [x] **Durable persistence established as sole correctness authority (no memory/distributed lock dependence).**
- [x] **Dual-layer guard required (semantic state predicates + opaque concurrency revisions).**
- [x] **No physical database vendor, SQL syntax, or schema column frozen.**
- [x] **Durable commit established as semantic linearization point.**
- [x] **Control-plane contention isolated from business execution failure.**
- [x] **Unknown commit outcomes resolved via authoritative re-query (never assumed failure).**
- [x] **Short-lived persistence transactions with zero network I/O inside transaction scope.**
- [x] **Independent parallel task concurrency preserved without workflow-wide locks.**
- [x] **Distributed lock managers (Redis/ZooKeeper) explicitly rejected for V1 correctness.**
- [x] **Universal pessimistic locking rejected; physical optimizations deferred to ADR-020.**
- [x] **Multi-entity concurrency guard rule established for dependent transitions.**
- [x] **Active attempt failure during drain settles as `FAILED` with no retry; unstarted tasks settle as `CANCELLED`.**
- [x] **Workflow terminalization guarded by complete set-predicate serialization point check.**
- [x] **Derived counters classified as non-authoritative performance caches.**
- [x] **Pre-ownership vs. post-ownership broker message loss distinctions accurately preserved.**
- [x] **Recovery sweeps use identical OCC rules without special recovery locks.**
- [x] **Strict boundaries maintained with ADR-015, ADR-018, ADR-020, ADR-023, and ADR-025.**
