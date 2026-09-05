# ADR-005 — Workflow Task Scheduling & Dispatch Architecture

*   **Status**: Approved (not Frozen)
*   **Last Updated**: 2026-09-05
*   **Deciders**: Project Owner
*   **Domain**: Execution (Task Scheduling & Progression)
*   **Criticality**: Critical
*   **Relationships**:
    *   **Depends On**: [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md), [ADR-004: Workflow Validation Strategy](adr-004-workflow-validation-strategy.md)
    *   **Enables**: [ADR-006: Workflow Execution State Machine](00-architecture-decision-register.md#L201), [ADR-007: Task Execution Lifecycle & Attempt Model](00-architecture-decision-register.md#L202)
    *   **Related To**: [ADR-008: Worker Coordination & Liveness Model](00-architecture-decision-register.md#L203), [ADR-009: Task Routing Strategy](00-architecture-decision-register.md#L204), [ADR-012: Recovery Strategy](00-architecture-decision-register.md#L211), [ADR-013: Consistency & Concurrency Strategy](00-architecture-decision-register.md#L212)

---

## 1. Purpose
This document defines the task scheduling architecture for NexusFlow. It establishes how the engine evaluates task dependency eligibility, responds to execution lifecycle transitions, enforces single-winner atomic progression, and yields runnable work to a transport-neutral dispatch boundary. It guarantees that task execution progression is correct under concurrency, immune to duplicate signals, and reconcilable from authoritative execution state together with the immutable validated workflow definition and canonical topology, without coupling scheduling to volatile worker coordination or transport protocols.

---

## 2. Context
NexusFlow establishes workflow definitions through [ADR-001](adr-001-internal-workflow-specification.md) (IWS) and derives immutable, bidirectional dependency topology through [ADR-003](adr-003-canonical-workflow-graph-representation.md). [ADR-004](adr-004-workflow-validation-strategy.md) validates this topology, guaranteeing an acyclic directed graph (DAG) with unique task identities, valid references, and support for multiple roots, multiple leaves, and disconnected components.

At runtime, Workflow Executions and Task Executions are instantiated. The engine must determine when tasks become eligible to execute based on upstream dependencies. Without a disciplined scheduling architecture:
* Systems fall back to continuous whole-graph polling ($O(V + E)$ full scans), repeatedly performing work proportional to the active workflow graph even when only a small subset of task states changed, potentially increasing compute/storage reads as workflow count and DAG size grow.
* Concurrency races in fan-in joins and diamond DAGs cause double-progression anomalies, dispatching identical tasks to multiple workers simultaneously.
* Ephemeral event streams are mistakenly treated as the source of truth, causing workflow executions to stall if an in-memory notification is dropped during an ungraceful shutdown.
* Schedulers leak into worker protocols (push vs. pull, queues, heartbeats), tangling dependency evaluation with network communication.

ADR-005 establishes the transition-driven progression model and defines the boundary between dependency eligibility and physical dispatch.

---

## 3. Problem Statement
Given an immutable `Validated IWS`, its `Canonical Workflow Graph`, and authoritative durable execution states, how should NexusFlow deterministically determine which Task Executions are eligible to progress, react to relevant execution lifecycle transitions, produce runnable work without duplicate progression under concurrency, and hand runnable work to the dispatch subsystem while remaining correct across process restarts?

---

## 4. Requirements Covered
*   **Capability 3**: Task scheduling based on DAG dependency satisfaction.
*   **System Invariant (KT-8)**: "No task can be executed until all its declared dependencies have successfully completed."
*   **Core Principle**: Correctness Over Performance.
*   **Core Principle**: Explicit Behaviour Over Implicit Behaviour.
*   **Core Principle**: Deterministic State Transitions.
*   **Core Principle**: Recovery as a First-Class Capability.
*   **Core Principle**: Clear Ownership and Separation of Responsibilities.

---

## 5. Constraints
*   **Decoupled from Transport**: The scheduler must not depend on specific queue technologies, broker protocols, threading models, or worker transport mechanisms (push vs. pull).
*   **Immutable Definition Topology**: The scheduler must not mutate the `Canonical Workflow Graph` or store execution-specific state (e.g., remaining dependency counters, worker assignments) within graph structures.
*   **Authoritative Durable State**: The scheduler must not act as an independent source of truth; all scheduler decisions must derive from durable workflow and task execution states.
*   **Concurrency Safety**: Logical progression of any task must be atomic and single-winner; multiple concurrent evaluation triggers must never produce duplicate runnable work.
*   **Single-Developer V1 Feasibility**: The architecture must avoid distributed consensus, actor frameworks, distributed locks, or complex event-sourcing infrastructure.

---

## 6. Goals
*   Establish transition-driven scheduling with authoritative state re-evaluation as the primary progression model for normal operation.
*   Scope normal transition evaluation strictly to direct downstream dependents (`dependents[T]`), avoiding recursive descendant traversals or repeated whole-graph scans.
*   Adopt an eager initialization model where exactly one logical `TaskExecution` per `TaskDefinition` is created at workflow start in a pre-runnable condition.
*   Enforce AND-style dependency satisfaction: a task is dependency-eligible if and only if all declared upstream prerequisites reach a dependency-satisfying lifecycle outcome.
*   Define a single-winner atomic progression boundary that guarantees idempotent evaluation across duplicate, delayed, or out-of-order signals.
*   Provide a deterministic state-based reconciliation capability allowing the scheduler to rebuild readiness and resume progression after an engine restart.
*   Establish a clean separation between semantic eligibility (progressing to an execution-eligible/runnable condition) and physical worker dispatch (ADR-008 / ADR-009).

---

## 7. Non-Goals
*   Defining concrete Task Execution state names, enum labels, or internal attempt lifecycles (deferred to [ADR-007](00-architecture-decision-register.md#L202)).
*   Defining concrete Workflow Execution state machines, cancellation propagation, or terminal workflow failure aggregation (deferred to [ADR-006](00-architecture-decision-register.md#L201)).
*   Selecting database transaction types, row-level locks, compare-and-swap primitives, or concurrency control mechanisms (deferred to [ADR-013](00-architecture-decision-register.md#L212)).
*   Selecting worker queues, message brokers, network protocols, worker liveness monitoring, or heartbeat tracking (deferred to [ADR-008](00-architecture-decision-register.md#L203)).
*   Defining worker selection algorithms, task routing strategies, or worker capability matching (deferred to [ADR-009](00-architecture-decision-register.md#L204)).
*   Designing engine restart detection, storage scanning loops, or recovery orchestration (deferred to [ADR-012](00-architecture-decision-register.md#L211)).
*   Designing scheduling priorities, FIFO queues, weighted fairness, rate limits, tenant quotas, or concurrency throttling caps.

---

## 8. Candidate Solutions

### Alternative A: Repeated Whole-Graph Polling
A scheduler loop that periodically loads all active workflows, scans all task definitions and dependency edges in $O(V + E)$ time, checks whether dependencies are met, and schedules runnable tasks.
*   *Pros*: Conceptually simple; resilient to missed in-memory events; naturally self-healing for reconciliation.
*   *Cons*: Repeatedly performs work proportional to the active workflow graph even when only a small subset of task states changed, potentially increasing compute/storage reads as workflow count and DAG size grow; delays progression to the polling period.

### Alternative B: Transition-Driven Scheduling with Authoritative State Re-Evaluation [Selected]
Progression is triggered by authoritative task lifecycle transitions. When task $T$ completes, the scheduler queries `dependents[T]` from the `Canonical Workflow Graph` and inspects the durable state of each child's prerequisites (`dependencies[C]`). Eligible tasks are progressed via an atomic single-winner operation.
*   *Pros*: Highly efficient; evaluates only affected direct dependents; state re-evaluation is idempotent and robust to duplicate, delayed, and out-of-order triggers; missed progression can be reconstructed through reconciliation; simple to reason about under concurrency.
*   *Cons*: Requires a reliable notification mechanism (function call, event, callback) to trigger the transition path; requires direct-dependent state lookups.

### Alternative C: Transition-Driven Scheduling with Mutable Remaining-Dependency Counters
Each task execution stores an integer counter initialized to `indegree(T)`. When an upstream task succeeds, a notification decrements `remaining[T] -= 1`. When `remaining[T] == 0`, the task is progressed.
*   *Pros*: Constant-time $O(1)$ readiness evaluation upon upstream completion.
*   *Cons*: Extremely vulnerable to concurrency races (lost updates, double decrements on duplicate signals); difficult to recover from crashes without full state recalculation; counter drift creates permanent workflow deadlocks.

### Alternative D: Precomputed Sequential Topological Execution Plan
The engine computes a static topological ordering (e.g., $[T_1, T_2, T_3]$) and executes tasks strictly in array sequence.
*   *Pros*: Trivial implementation; no concurrency controls required.
*   *Cons*: Completely serializes execution, destroying DAG concurrency; cannot handle independent parallel branches, fan-out, or disconnected components; violates fundamental orchestrator requirements.

### Alternative E: Fully Event-Sourced Scheduler State Machine
The scheduler maintains no durable entity state; all readiness and progression are computed purely by replaying an immutable append-only event stream from offset zero.
*   *Pros*: Complete auditability; replayable debugging.
*   *Cons*: High implementation and cognitive overhead; requires event compaction, snapshotting, and event-stream infrastructure; excessive complexity for a V1 solo-developer architecture.

---

## 9. Detailed Evaluation of Every Candidate

| Evaluation Criteria | Alt A: Whole-Graph Polling | Alt B: Transition-Driven (Selected) | Alt C: Mutable Counters | Alt D: Sequential Topological | Alt E: Event-Sourced |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Concurrency Safety** | Medium (race on tick) | **Highest** (single-winner CAS) | Lowest (lost updates) | High (no concurrency) | High |
| **Crash Recoverability** | High | **Highest** (state reconstructible)| Low (counter drift) | High | High (replay required)|
| **Execution Efficiency** | Poor ($O(V + E)$ every tick)| **High** ($O(\text{affected})$) | Highest ($O(1)$ decrement) | High | Medium (replay cost) |
| **DAG Parallelism** | High | **High** | High | **None** (serialized) | High |
| **Implementation Simplicity**| High | **High** (clean boundaries) | Medium | Highest | Lowest (high overhead) |
| **Idempotency / Resilience**| High | **Highest** (state-driven) | Low (duplicate signals fail) | Low | High |
| **Solo-Developer Feasibility**| High | **High** | Medium | High | Low |

---

## 10. Decision
NexusFlow adopts **Alternative B: Transition-Driven Scheduling with Authoritative State Re-Evaluation and Single-Winner Atomic Progression**.

### 10.1 Conceptual Architecture & Flow

```
   Validated IWS (ADR-001) + Canonical Workflow Graph (ADR-003)
                                 +
        Authoritative Durable State (Workflow & Task Executions)
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ADR-005 Scheduler Core                       │
│                                                                 │
│  1. Ingestion of Transition Trigger (or Initial Root Discovery) │
│  2. Identify Candidate Tasks: dependents[T] (Direct Children)   │
│  3. Unified Eligibility Evaluation:                             │
│     - Workflow permits progression?                             │
│     - Task in pre-runnable condition?                           │
│     - All dependencies[C] in dependency-satisfying state?       │
│  4. Attempt Single-Winner Atomic Progression                    │
│     - Pre-runnable condition -> Runnable condition             │
│     - Losers safely discard (idempotent no-op)                  │
└────────────────────────────────┬────────────────────────────────┘
                                 │
                                 ▼
         Transport-Neutral Runnable Work / Dispatch Intent
                                 │
                                 ▼
            ADR-008 (Coordination) / ADR-009 (Routing)
```

### 10.2 Authoritative State Ownership vs. Derived Scheduler State
*   **Authoritative Durable State**: Workflow Execution lifecycle state, Task Execution lifecycle state, and Task Execution identity are authoritative and owned by later execution and persistence ADRs.
*   **Immutable Definitions**: `Validated IWS` and `Canonical Workflow Graph` are strictly read-only and contain no execution metadata.
*   **Derived Ephemeral State**: Any scheduler-local structures (readiness caches, candidate queues, dispatch buffers) are purely derived and must be 100% reconstructible from authoritative durable state.

### 10.3 Eager Task Execution Instantiation
When a Workflow Execution is initialized, the engine eagerly instantiates exactly one logical `TaskExecution` record per `TaskDefinition`.
*   Each `TaskExecution` begins in an initial lifecycle condition indicating it exists but has not yet become runnable.
*   *Boundary Guard*: ADR-005 relies on the presence of these records to execute atomic progression; the exact concrete state name and lifecycle enum are owned by [ADR-007](00-architecture-decision-register.md#L202).
*   *Domain Invariant*: Exactly one logical `TaskExecution` exists per `TaskDefinition` per `WorkflowExecution`. Retries spawn `ExecutionAttempt` records under ADR-007 and never create duplicate `TaskExecution` entities.

### 10.4 Direct-Dependent Evaluation Scope
Upon an upstream task $T$ reaching a dependency-satisfying state, the scheduler inspects **direct dependents only** (`dependents[T]`). It does **not** recursively traverse all descendants. Transitive dependents ($C \to D$) are evaluated naturally when child $C$ eventually executes and reaches a terminal state.

### 10.5 Unified Eligibility Evaluation
The scheduler evaluates task eligibility using a single, uniform concept across initial scheduling, transition-driven progression, and recovery reconciliation:
A task $T$ is eligible for progression if and only if:
1.  The parent Workflow Execution is in a lifecycle condition that permits task progression.
2.  The `TaskExecution` has not already progressed beyond its initial pre-runnable lifecycle condition.
3.  Every declared prerequisite $U \in \text{dependencies}[T]$ is in a task-level state classified as **dependency-satisfying**.
4.  No scheduler-level semantic constraint prevents progression.

### 10.6 Dependency Satisfaction Semantics (V1)
*   **Semantic Rule**: All declared prerequisites must reach a dependency-satisfying task-level state before a dependent task becomes eligible (strict AND-semantics).
*   **V1 Classification**: Successful task completion is the **only** dependency-satisfying outcome. Active execution, retrying attempts, permanent failures, timeouts, and cancellations do **not** satisfy dependencies.
*   **Abstraction**: ADR-005 evaluates the abstract property `is_dependency_satisfied(task_state)`, allowing future ADRs to introduce conditional or cleanup branches without altering core graph traversal logic.

### 10.7 Rejection of Mutable Dependency Counters
Remaining-dependency counters are **not authoritative** in V1. Eligibility is derived directly by re-evaluating the durable states of upstream prerequisites in `dependencies[T]` ($O(\text{indegree}(T))$). Derived counters may only be introduced in the future as non-authoritative performance optimizations if they remain reconstructible and concurrency-safe.

### 10.8 Single-Winner Atomic Progression
When multiple concurrent events or threads evaluate the same task (e.g., in fan-in joins or diamond topologies), the scheduler requires an atomic single-winner progression boundary:
*   At most one evaluation may commit the transition of a `TaskExecution` from its pre-runnable condition to runnable.
*   Concurrent evaluations that evaluate the same task must observe that progression has already occurred or safely fail their conditional progression attempt.
*   *Mechanism Decoupling*: ADR-005 defines the logical single-winner invariant; the concrete mechanism (e.g., conditional compare-and-swap, row-level locks, transactional updates) is deferred to [ADR-013](00-architecture-decision-register.md#L212).

### 10.9 Separation of Eligibility from Dispatch
*   **Eligibility (ADR-005)**: The semantic determination that a task is logically in an execution-eligible/runnable condition.
*   **Dispatch (ADR-008 / ADR-009)**: The physical handoff of runnable work to workers.
*   If dispatch fails, workers are offline, or routing systems are unavailable, the task remains in its runnable condition. The scheduler does not revoke eligibility or recreate `TaskExecution` records.
*   ADR-005 emits a transport-neutral **Runnable Work / Dispatch Intent** descriptor identifying the workflow execution, task execution, task definition, and activity type. Push vs. pull protocols and queue mechanics are deferred to ADR-008.

### 10.10 Deterministic Eligibility vs. Non-Deterministic Execution
*   **Deterministic**: For identical workflow topology and durable execution states, the derived set of eligible tasks is strictly deterministic.
*   **Non-Deterministic**: The wall-clock dispatch order, worker execution order, and completion timing among independent concurrent tasks are intentionally non-deterministic. Topological order is an indexing tool, not an execution sequence.

### 10.11 State-Based Reconciliation Capability
The scheduler must provide a deterministic reconciliation capability that re-evaluates all tasks for a workflow execution against durable state.
*   Used during startup recovery ([ADR-012](00-architecture-decision-register.md#L211)) to resume workflows after an ungraceful engine shutdown.
*   Guarantees that scheduler correctness does not depend on ephemeral in-memory event delivery.
*   *Periodic Background Sweeping*: A low-frequency background sweeper during healthy operation is recognized as an optional operational self-healing feature, but is **deferred** until operational needs justify it.

---

## 11. Decision Rationale
1. **Preventing Double-Progression Anomalies**: In fan-in ($A, B \to D$) and diamond ($A \to B, C \to D$) topologies, $B$ and $C$ frequently finish concurrently. Without single-winner atomic progression, two worker processes would both progress $D$, resulting in duplicate execution, corrupted outputs, or wasted compute.
2. **Robustness Against Lost and Duplicate Signals**: In asynchronous distributed environments, notifications can arrive duplicated, delayed, or out of order. By re-evaluating durable upstream states rather than blindly decrementing counters, the scheduler is naturally idempotent: receiving a duplicate completion signal simply re-verifies that dependencies are already satisfied and safely halts.
3. **Eliminating Counter Drift**: Mutable counters (`remaining -= 1`) are notorious for race conditions where concurrent updates overwrite each other (lost updates). Checking the actual states of declared prerequisites incurs $O(\text{indegree}(T))$ authoritative-state evaluation rather than a constant-time counter update, trading additional reads for simpler correctness and recovery semantics.
4. **Clean Operational Boundaries**: Decoupling eligibility from dispatch ensures that transient worker outages do not corrupt orchestration semantics. A workflow can progress through its logical DAG regardless of worker queue availability.
5. **Eager Instantiation Simplifies State Accounting**: Having all logical `TaskExecution` entities exist durably from Workflow Execution initialization enables a conditional single-winner transition from the initial non-runnable lifecycle condition, provides immediate visibility into the entire workflow plan via APIs, and simplifies downstream cancellation marking.

---

## 12. Tradeoffs
*   **State Re-evaluation vs. O(1) Decrements**: Checking upstream task states incurs $O(\text{indegree}(T))$ authoritative-state evaluation rather than a constant-time counter update, trading additional reads for simpler correctness and recovery semantics.
*   **Direct-Dependent Scoping vs. Global Reconciliation**: Scoping normal transitions to direct dependents requires an explicit reconciliation hook for crash recovery. We accept this minor separation of concerns to avoid the performance overhead of repeated whole-graph scans.
*   **Strict AND-Semantics vs. Expressive Branching**: Limiting V1 dependency satisfaction to successful completion defers conditional branching and cleanup tasks, but keeps V1 scheduling rock-solid, easy to reason about, and implementable by a single developer.

---

## 13. Consequences

### Codebase & Architectural Impact
*   **ADR-006 / ADR-007 Independence**: The scheduler depends only on abstract lifecycle predicates (`workflow_allows_progression`, `is_dependency_satisfied`), preventing tight coupling to concrete state enum values.
*   **Dispatch Decoupling**: Downstream worker coordination ([ADR-008](00-architecture-decision-register.md#L203)) and task routing ([ADR-009](00-architecture-decision-register.md#L204)) can consume transport-neutral dispatch intents without scheduling logic leaking into transport code.
*   **Crash Resilience**: Crash recovery ([ADR-012](00-architecture-decision-register.md#L211)) does not need complex event-replay infrastructure; invoking the scheduler's reconciliation method over active workflows fully restores progress.

### Performance Impact
*   Normal scheduling scales with graph fan-out and in-degree ($O(\sum \text{indegree}(C))$), avoiding $O(V + E)$ whole-graph scans on high-frequency execution paths.
*   Eliminating mutable counter locks reduces database write contention on join nodes.

---

## 14. Failure Modes

| Failure Mode | Symptom / Consequence | Owning ADR | Mitigation |
| :--- | :--- | :--- | :--- |
| **Duplicate Completion Signal** | Downstream dependent evaluated multiple times | ADR-005 | Idempotent state evaluation + single-winner atomic progression safely drops second attempt. |
| **Out-of-Order Signals** | Signal for downstream arrives before upstream persists | ADR-005 | Authoritative state check: only unlocks if upstream state is durably dependency-satisfying. |
| **Stale Signal Replay** | Replaying an old signal for an already completed task | ADR-005 | Pre-runnable condition check: tasks already progressed are ignored. |
| **Concurrent Fan-In Race** | Multiple upstream completions evaluate same join node | ADR-005 / ADR-013 | Single-winner atomic progression boundary guarantees exactly one commit. |
| **Crash Post-Success / Pre-Progression** | Upstream marked complete; engine crashes before child evaluated | ADR-005 / ADR-012 | Startup reconciliation scans active workflows, evaluates state, and progresses eligible children. |
| **Crash Post-Progression / Pre-Dispatch** | Task marked runnable; engine crashes before worker handoff | ADR-005 / ADR-012 | Recovery detects tasks in runnable condition and re-emits dispatch intents. |
| **Worker Subsystem Outage** | Dispatch handoff fails or no workers available | ADR-005 / ADR-008 | Task remains in runnable condition; scheduler does not revoke eligibility or duplicate records. |
| **Upstream Permanent Failure** | Dependent task can never satisfy dependencies | ADR-005 / ADR-006 | Dependent remains un-progressed; ADR-006/007 handle downstream blocked state transitions. |
| **Workflow Cancelled Mid-Progression** | Workflow cancellation starts while task is being evaluated | ADR-005 / ADR-006 / ADR-013 | Progression must not successfully commit if the containing Workflow Execution no longer permits scheduling; ADR-013 defines the consistency mechanism required to prevent races between workflow lifecycle changes and task progression. |
| **Large Fan-Out Burst** | Completing task exposes hundreds of runnable children | ADR-005 / ADR-008 | Large fan-out can create bursts of eligibility progression and dispatch handoffs. Downstream execution/dispatch architecture must provide an appropriate capacity/backpressure strategy; concrete batching and queueing mechanisms are deferred. |
| **Scheduler-Local Cache Loss** | In-memory candidate queue lost on restart | ADR-005 | All scheduler state is derived; rebuilt cleanly from durable state via reconciliation. |

---

## 15. Debugging Considerations
*   **Progression Diagnostic Tracing**: Structured logs must record why a task was progressed (e.g., `Task "payment" became RUNNABLE triggered by completion of "fraud_check"`).
*   **Ineligibility Inspection**: The scheduler must provide a queryable diagnostic mechanism explaining why a task is not runnable, detailing which specific upstream dependencies in `dependencies[T]` remain unsatisfied.
*   **Single-Winner Contention Logging**: Debug logs should record when a concurrent evaluation safely lost the progression race (e.g., `Progression skipped for "order_complete": task already progressed by concurrent handler`).

---

## 16. Testing Considerations
Scheduler testing must cover the following scenarios (coordinated with [ADR-021](00-architecture-decision-register.md#L220)):
1.  **Topology Progression**: Single root, multiple roots, linear chain, fan-out ($1 \to N$), fan-in ($N \to 1$), diamond DAG ($A \to B, C \to D$), and disconnected DAG components.
2.  **Concurrency & Races**: Concurrent upstream completions in diamond and fan-in topologies; verified that join nodes progress **exactly once**.
3.  **Signal Idempotency**: Duplicate, delayed, and out-of-order completion signals; verified that no duplicate runnable work is created.
4.  **Failure Isolation**: Permanent failure of upstream task prevents downstream progression; retrying upstream task withholds downstream progression until task-level success.
5.  **Lifecycle Gating**: Task progression aborted when workflow is in a non-schedulable lifecycle condition.
6.  **Dispatch Decoupling**: Verification that tasks remain runnable when dispatch handoff fails or encounters mock transport exceptions.
7.  **Reconciliation & Crash Simulation**: Simulating engine crash between upstream completion and downstream progression; verified that reconciliation restores correct runnable state.
8.  **Identity Invariant**: Verification that exactly one `TaskExecution` exists per `TaskDefinition` across all lifecycle operations.

---

## 17. Operational Considerations
*   **Read Load on Joins**: For tasks with large in-degrees, state re-evaluation reads multiple task records. In-memory caching of active execution states (invalidated on state changes) can optimize reads without compromising durability.
*   **Fan-Out Throttling**: Large fan-out can create bursts of eligibility progression and dispatch handoffs. Downstream execution/dispatch architecture must provide an appropriate capacity/backpressure strategy; concrete batching and queueing mechanisms are deferred.
*   **Zero In-Memory Truth**: Because no critical orchestration state lives in scheduler memory, orchestrator nodes can be restarted at any time without data loss or stranded workflows.

---

## 18. Maintenance Considerations
*   **Adding Conditional Dependencies**: When conditional execution or run-on-failure policies are introduced in future ADRs, they integrate by expanding the `is_dependency_satisfied` predicate, preserving the existing transition-driven scheduler architecture.
*   **Introducing Priorities**: Priority or fairness queues can be introduced at the dispatch boundary without modifying the core dependency eligibility engine.

---

## 19. Future Evolution
*   **Advanced Branching & Cleanup Tasks (V2)**: Support for `run_if: always` or `run_if: failed` will be enabled by extending the dependency satisfaction predicate.
*   **Periodic Self-Healing Sweeper (V2)**: If operational telemetry reveals dropped transitions in distributed setups, a background reconciliation sweeper can be scheduled without altering progression logic.
*   **Clustered Scheduler Scaling (ADR-025)**: Multiple orchestrator instances can safely run the transition-driven scheduler concurrently using the single-winner atomic progression boundary.

---

## 20. Rejected Alternatives
*   **Rejected Mutable Dependency Counters**: Discarded due to high vulnerability to lost updates and counter drift under concurrent completion signals.
*   **Rejected Whole-Graph Polling as Primary Scheduler**: Discarded due to repeated unnecessary work across active graphs and delayed progression.
*   **Rejected Sequential Topological Execution**: Discarded because it destroys workflow parallelism and fails to support disconnected DAGs.
*   **Rejected Full Event-Sourced Scheduler**: Discarded as gross overengineering for a V1 single-developer engine.
*   **Rejected Lazy Task Execution Creation**: Discarded because eager creation provides superior API visibility, clean atomic transitions, and simplified cancellation tracking.

---

## 21. Decision Evolution
*   *2026-09-05*: Initial draft establishing transition-driven scheduling, authoritative state re-evaluation, single-winner atomic progression, eager task creation, and separation from dispatch.

---

## 22. Common Misconceptions
*   *Misconception*: "The scheduler must run in a single thread to avoid concurrency races."
    *   *Correction*: Concurrency is handled at the state boundary via single-winner atomic progression. Multi-threaded or asynchronous task completion handlers can safely evaluate the same task concurrently.
*   *Misconception*: "Topological sort defines the exact order in which tasks must execute."
    *   *Correction*: Topological sorting is an indexing structure. Workflows execute concurrently based on state readiness; independent tasks execute in arbitrary order.
*   *Misconception*: "Marking a task runnable means it has been delivered to a worker."
    *   *Correction*: Eligibility (advancing to a runnable condition) is a semantic orchestrator state. Delivery is a physical transport concern managed by the dispatch subsystem.
*   *Misconception*: "If a task attempt fails, downstream tasks are immediately marked failed."
    *   *Correction*: The scheduler inspects task-level lifecycle outcomes, not individual attempt failures. Downstream tasks wait while retries occur.

---

## 23. Open Questions
*   *Concrete Dispatch Descriptor Serialization*: What exact wire format should package dispatch intents for worker queues? *(Deferred to [ADR-008](00-architecture-decision-register.md#L203) / [ADR-009](00-architecture-decision-register.md#L204)).*
*   *Periodic Reconciliation Trigger Policy*: Under what operational thresholds should a background reconciliation sweep run in production? *(Deferred to [ADR-012](00-architecture-decision-register.md#L211) and operational policy).*

---

## 24. Interview Discussion

### Why transition-driven scheduling instead of full DAG polling?
Full DAG polling performs $O(V + E)$ scans on every tick, reading tasks whose state has not changed and performing redundant compute/storage reads. Transition-driven scheduling reacts immediately to state changes, scoping evaluation strictly to direct dependents ($O(\text{affected})$), yielding efficient progression and high scaling responsiveness.

### Why keep a reconciliation capability if transition-driven scheduling is used?
Transition-driven scheduling relies on notification triggers. If the engine crashes between a task completing and its child being scheduled, the in-memory trigger is lost. State-based reconciliation allows the engine to inspect durable records on restart, deduce that prerequisites are met, and recover without missing progress.

### Why not use mutable remaining-dependency counters?
Decrementing counters across concurrent events without distributed locks creates lost-update bugs. For example, if two prerequisites finish simultaneously, both read 2 and write 1 instead of 0, permanently deadlocking the workflow. Re-evaluating actual prerequisite states against durable records incurs $O(\text{indegree}(T))$ authoritative-state evaluation rather than a constant-time counter update, trading additional reads for simpler correctness and recovery semantics, and is completely immune to lost-update anomalies.

### How do fan-in joins and diamond graphs avoid duplicate progression?
Through single-winner atomic progression. When multiple prerequisites finish simultaneously, both handlers evaluate the join task and find it eligible. However, progressing the task requires an atomic conditional state transition from the initial non-runnable condition. Exactly one handler succeeds; all others observe that progression already committed and safely exit.

### Why eagerly create all Task Executions at workflow initialization?
Eager instantiation ensures all logical TaskExecution entities exist durably from Workflow Execution initialization. This enables a conditional single-winner transition from the initial non-runnable lifecycle condition, gives monitoring APIs immediate visibility into the complete workflow plan, and allows downstream cancellation or failure marking without dynamic record generation.

### Why separate eligibility from worker dispatch?
A task can be semantically eligible to run even if zero workers are online, queues are congested, or the network is partitioned. Conflating eligibility with dispatch would mean transport failures roll back workflow progress. Keeping them separate guarantees that dependency satisfaction is durable and independent of transport health.

---

## 25. References
*   [ADR-001: Internal Workflow Specification](adr-001-internal-workflow-specification.md)
*   [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md)
*   [ADR-004: Workflow Validation Strategy](adr-004-workflow-validation-strategy.md)
*   [Architecture Decision Register](00-architecture-decision-register.md)

---

## 26. Traceability
*   **Depends On**: [ADR-003](adr-003-canonical-workflow-graph-representation.md), [ADR-004](adr-004-workflow-validation-strategy.md)
*   **Enables**: [ADR-006](00-architecture-decision-register.md#L201), [ADR-007](00-architecture-decision-register.md#L202)
*   **Related To**: [ADR-008](00-architecture-decision-register.md#L203), [ADR-009](00-architecture-decision-register.md#L204), [ADR-012](00-architecture-decision-register.md#L211), [ADR-013](00-architecture-decision-register.md#L212)

---

## 27. Decision Validation Checklist
*   [x] Is the problem statement decoupled from specific database/broker technologies?
*   [x] Are the functional requirements (FRs) and non-functional requirements (NFRs) traced?
*   [x] Were at least two realistic candidate designs critically evaluated?
*   [x] Are the tradeoffs clear (what are we giving up for simplicity or correctness)?
*   [x] Does the design preserve all mapped system invariants?
*   [x] Does this decision avoid introducing tight coupling between modules?
*   [x] Are the potential failure modes mapped?
*   [N/A] Is there a clear explanation of how this design behaves during a graceful shutdown? *(Justification: Decoupled scheduler logic; graceful draining of dispatchers is owned by ADR-017).*
*   [x] Are the debugging strategies defined?
*   [x] Does the testing strategy explain how to simulate failures and recovery?
*   [x] Are the performance limits and resource footprints qualitatively identified?
*   [N/A] Is the future evolution path to HA or multi-tenant deployment explained? *(Justification: Single-node V1 focus; clustered scheduler coordination is deferred to ADR-025).*
*   [x] Can this decision be defended during an SDE-2 engineering review?
