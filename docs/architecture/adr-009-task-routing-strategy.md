# ADR-009 — Task Routing Strategy

*   **Status**: Approved
*   **Last Updated**: 2026-09-05
*   **Deciders**: Project Owner
*   **Domain**: Execution (Task Routing & Worker Matching)
*   **Criticality**: Supporting
*   **Relationships**:
    *   **Depends On**: [ADR-008: Worker Coordination & Liveness Model](adr-008-worker-coordination-and-liveness-model.md)
    *   **Related To**: [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md), [ADR-005: Workflow Task Scheduling & Dispatch Architecture](adr-005-workflow-task-scheduling-and-dispatch-architecture.md), [ADR-007: Task Execution Lifecycle & Attempt Model](adr-007-task-execution-lifecycle-and-attempt-model.md), [ADR-011: State Persistence Strategy](00-architecture-decision-register.md#L210), [ADR-012: Recovery Strategy](00-architecture-decision-register.md#L211), [ADR-013: Consistency & Concurrency Strategy](00-architecture-decision-register.md#L212), [ADR-014: History & Audit Model](00-architecture-decision-register.md#L213), [ADR-016: Observability Architecture](00-architecture-decision-register.md#L221), [ADR-019: Project Modularity & Service Boundaries](00-architecture-decision-register.md#L222), [ADR-020: Technology Selection Strategy](00-architecture-decision-register.md#L223), [ADR-022: Security Model (Version 1)](00-architecture-decision-register.md#L225)

---

## 1. Purpose
This document defines the task routing architecture for NexusFlow. It establishes how a logical `TaskExecution` in the `RUNNABLE` condition is matched to an eligible `WorkerSessionId` capable of executing its declared Activity Type. It standardizes a two-stage capability-based matching relation, ensures push and pull symmetry, enforces that routing decisions remain strictly advisory without creating execution attempts, defines passive backpressure when compatible workers are absent, and establishes the ownership commit revalidation contract.

---

## 2. Context
NexusFlow establishes workflow definitions and static DAG structures via [ADR-001](adr-001-internal-workflow-specification.md) and [ADR-003](adr-003-canonical-workflow-graph-representation.md). [ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md) governs scheduling, evaluating task dependency completeness and transitioning execution-eligible tasks into the `RUNNABLE` condition. [ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md) defines the task and attempt lifecycles, mandating that an `ExecutionAttempt` is created **only** when worker execution ownership is authoritatively established. [ADR-008](adr-008-worker-coordination-and-liveness-model.md) establishes worker runtime coordination, ephemeral worker registration, session liveness, capability advertisement (Activity Types), and the abstract Three-Phase Ownership Handshake:
1. **Phase 1 (Candidate / Offer)**: Identifying a compatible worker candidate.
2. **Phase 2 (Ownership Commit)**: Authoritatively granting execution ownership to exactly one worker session.
3. **Phase 3 (Execution Start)**: Observing worker execution start.

ADR-009 sits directly between ADR-005 and ADR-008, operating exclusively within **Phase 1 (Candidate / Offer)**:
* **ADR-005 Boundary**: The scheduler determines *when* a task is semantically eligible to execute (`RUNNABLE`). ADR-009 does not evaluate DAG dependencies or workflow progression rules.
* **ADR-008 Boundary**: ADR-008 provides the active worker registry, session liveness status, and executes Phase 2 Ownership Commit. ADR-009 provides the candidate selection logic that identifies which worker session to pair with a runnable task.
* **ADR-007 Boundary**: Routing selection is strictly non-authoritative. It creates no `ExecutionAttempt`, acquires no lease, reserves no persistent state, and consumes zero retry budget.

---

## 3. Problem Statement
How should NexusFlow match `RUNNABLE` task executions to live, eligible worker sessions capable of executing their required Activity Types, ensuring that routing remains transport-neutral across push and pull models, deterministic in eligibility rules, resilient to worker churn, and strictly decoupled from authoritative attempt creation?

---

## 4. Requirements Covered
*   **Capability 3**: Task Scheduling & Dispatch Integration.
*   **System Invariant (KT-8)**: "No task can be executed until all its declared dependencies have successfully completed." (Respected by routing only `RUNNABLE` tasks).
*   **System Invariant (KT-8)**: "At most one authoritative active ExecutionAttempt may exist for a TaskExecution." (Preserved by routing remaining advisory until single-winner commit).
*   **Core Principle**: Correctness Over Performance.
*   **Core Principle**: Explicit Behaviour Over Implicit Behaviour.
*   **Core Principle**: Technology Independence Before Implementation.
*   **Core Principle**: Clear Ownership and Separation of Responsibilities.
*   **Core Principle**: Simplicity Before Optimization.

---

## 5. Constraints
*   **Phase 1 Boundary**: Routing must remain strictly within Phase 1 (Candidate / Offer) of the ADR-008 handshake; it must never execute Phase 2 Ownership Commit directly.
*   **Zero Attempt Creation**: Routing decisions must not create an `ExecutionAttempt` or mutate `TaskExecution` state.
*   **Transport Neutrality**: The routing model must operate symmetrically across push-based orchestrator dispatch and pull-based worker polling without assuming message brokers, queues, or direct RPC.
*   **No Mandatory Worker Capacity Infrastructure**: In accordance with the deferred scope established in ADR-008, routing must not require complex slot accounting, active attempt counters, or global concurrency quotas in V1.
*   **Single-Developer V1 Feasibility**: Avoid distributed scheduler meshes, dynamic load-balancing telemetry, consistent hashing rings, or complex priority queues.

---

## 6. Goals
*   Define task routing as a symmetric, capability-based matching relation: $\text{Match}(\text{TaskExecution}, \text{WorkerSession})$.
*   Establish a two-stage routing architecture: Stage 1 (Strict Eligibility Filter) followed by Stage 2 (Replaceable Advisory Candidate Selection).
*   Enforce exact canonical Activity Type identity matching in V1, rejecting wildcards, subtype hierarchies, and implicit generic fallback workers.
*   Define worker eligibility using live session status, new-work acceptance, and capability matching.
*   Guarantee that the absence of a compatible worker leaves the task in `RUNNABLE` without penalty, timeout, or retry budget consumption.
*   Formally define the revalidation preconditions that must hold at ADR-008 Phase 2 Ownership Commit before an attempt can be created.
*   Ensure that routing state is entirely ephemeral and reconstructible across orchestrator crashes.

---

## 7. Non-Goals
*   Selecting physical transport frameworks (e.g., HTTP long-polling, gRPC streams, Redis, RabbitMQ) (deferred to [ADR-020](00-architecture-decision-register.md#L223)).
*   Freezing a mandatory candidate selection algorithm (e.g., round-robin) as an architectural requirement (deferred to implementation).
*   Implementing task priority queues, deadline-based scheduling, or FIFO fairness across tasks in V1.
*   Implementing worker capacity slot management, concurrency limits, or dynamic load balancing in V1.
*   Implementing worker affinity, sticky routing, data locality, or anti-affinity in V1.
*   Implementing dedicated worker pools or queue-per-activity-type broker topologies in V1.
*   Designing an independent routing microservice or process boundary (deferred to [ADR-019](00-architecture-decision-register.md#L222)).
*   Cryptographically verifying worker capability advertisements (deferred to [ADR-022](00-architecture-decision-register.md#L225)).

---

## 8. Candidate Solutions

### Alternative A: Direct Broadcast to All Workers
The orchestrator broadcasts every runnable task offer to all live workers simultaneously; workers race to claim execution.
*   *Mechanism*: Tasks are pushed or announced to all connected nodes; the first worker to complete Phase 2 Ownership Commit wins.
*   *Pros*: Minimal central routing logic.
*   *Cons*: Severe network and CPU overhead scaling with worker count; thundering-herd contention during ownership commit; worker nodes must filter irrelevant Activity Types locally.

### Alternative B: Mandatory Queue-Per-Activity-Type Topology
The routing layer maps each canonical Activity Type to a dedicated physical message broker queue.
*   *Mechanism*: Runnable tasks are published directly to a topic/queue; workers subscribe only to their supported queues.
*   *Pros*: Offloads routing and load distribution to an external message broker.
*   *Cons*: Tightly couples orchestration architecture to queue-based middleware before [ADR-020](00-architecture-decision-register.md#L223); prevents pure in-memory or direct-RPC deployments; complicates cross-cutting revalidation and crash recovery.

### Alternative C: Two-Stage Capability Matching with Replaceable Selection (Selected Architecture)
A logical, transport-neutral architecture consisting of a strict compatibility filter (Stage 1) followed by a pluggable, advisory candidate selection policy (Stage 2).
*   *Mechanism*: The orchestrator evaluates the symmetric relation $\text{Match}(T, W)$ across the ephemeral Worker Registry. If matches exist, a candidate selection policy picks one candidate pair $\langle T, W \rangle$ for Phase 2 Ownership Commit.
*   *Pros*: Completely transport-neutral; cleanly supports both push and pull; creates zero durable state; isolates advisory matching from atomic ownership commit; provides natural backpressure when workers are absent.
*   *Cons*: Requires a central registry query per routing evaluation; does not include built-in load-balancing telemetry in V1.

### Alternative D: Static Hash-Based Worker Affinity
Tasks are deterministically mapped to workers via consistent hashing of task identity.
*   *Mechanism*: Hash modulo over registered worker IDs determines assignment.
*   *Pros*: Deterministic placement; no routing cursor.
*   *Cons*: High sensitivity to worker churn; worker crashes force remapping; no awareness of worker draining status; unsuitable for dynamic heterogeneous pools.

---

## 9. Detailed Evaluation

| Evaluation Criteria | Alt A: Broadcast | Alt B: Queue-Per-Type | Alt C: Two-Stage Matching (Selected) | Alt D: Hash Affinity |
| :--- | :--- | :--- | :--- | :--- |
| **Transport Independence** | Moderate | Low (Broker-coupled) | **High (Push, pull, RPC, broker)** | Moderate |
| **Network & Contention Overhead** | High (Thundering herd) | Low | **Low (Targeted 1-to-1 candidate)** | Low |
| **Push / Pull Symmetry** | Poor (Push-biased) | Moderate (Pull-biased) | **High (Symmetric relation)** | Poor |
| **Fault Tolerance & Churn** | Moderate | High | **High (Revalidation at commit)** | Poor (Churn remaps) |
| **Recovery Simplicity** | High | Moderate | **High (Zero durable state)** | Moderate |
| **V1 Implementation Fit** | Poor | Poor (External infra) | **Optimal (Clean, lightweight)** | Moderate |

---

## 10. Decision

NexusFlow adopts **Alternative C: Two-Stage Capability-Based Matching Architecture with Advisory Candidate Selection**.

### 10.1 Two-Stage Routing Pipeline

```
┌────────────────────────────────────────────────────────────────────────┐
│ Stage 1: Strict Compatibility & Eligibility Filter                     │
│ Evaluates symmetric predicate Match(T, W) against Worker Registry.     │
│ Yields the Eligible Candidate Set C(T).                                │
└───────────────────────────────────┬────────────────────────────────────┘
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Stage 2: Replaceable Advisory Candidate Selection Policy               │
│ Selects one Candidate Match <T, W> from C(T).                          │
│ Implementation-level policy; non-authoritative; zero Attempt created.  │
└───────────────────────────────────┬────────────────────────────────────┘
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Handshake to ADR-008 Phase 2 (Ownership Commit)                        │
│ Atomically revalidates preconditions and creates Attempt in CLAIMED.   │
└────────────────────────────────────────────────────────────────────────┘
```

### 10.2 The Symmetric Matching Predicate: $\text{Match}(T, W)$
A `TaskExecution` $T$ and `WorkerSession` $W$ satisfy the routing matching relation $\text{Match}(T, W)$ if and only if all of the following hold:
1. **Task is Runnable**: $T.\text{status} == \text{RUNNABLE}$ (dependencies satisfied under [ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md)).
2. **Worker is Live**: $W$ is currently recognized as live under [ADR-008](adr-008-worker-coordination-and-liveness-model.md) control-plane liveness policy.
3. **Worker Accepts New Work**: $W.\text{accepting\_new\_work} == \text{true}$ (session is not draining or in maintenance).
4. **Exact Capability Match**: $W$ advertises the **exact canonical Activity Type identity** required by $T.\text{TaskDefinition}$.

### 10.3 Canonical Activity Type Matching
- **Exact Canonical Identity**: Compatibility requires exact equality of the canonical Activity Type identity.
- **No Wildcards / Hierarchies**: V1 rejects wildcard matching, subtyping, and capability inheritance.
- **No Implicit Generic Fallback**: Tasks are never routed to "generic" or "default" workers unless those workers explicitly advertise the exact Activity Type.
- Concrete syntax, casing rules, and namespace serialization are decoupled from this architecture.

### 10.4 Push / Pull Operational Symmetry
The matching relation is direction-agnostic and symmetric:
- **Push Mode (Orchestrator-Driven)**:
  Given a `RUNNABLE` task $T$, the router evaluates $\mathcal{C}(T) = \{ W \mid \text{Match}(T, W) \}$ and selects a candidate worker $W$.
- **Pull Mode (Worker-Driven)**:
  Given an idle worker session $W$ polling for work, the router evaluates $\mathcal{T}(W) = \{ T \mid \text{Match}(T, W) \}$ and selects a candidate task $T$.

Both execution directions share the exact same architectural filter and ownership commit boundary.

### 10.5 Advisory Candidate Output
- Routing emits an ephemeral **Candidate Match** pair: $\langle \text{TaskExecutionId}, \text{WorkerSessionId} \rangle$.
- **Non-Authoritative Contract**: A candidate match carries **zero execution authority**, acquires no lease, creates no `ExecutionAttempt`, and consumes no retry budget.
- Multiple routing cycles or concurrent routers may generate duplicate candidate matches safely.

### 10.6 Candidate Selection Policy (Stage 2)
- ADR-009 **defers** the exact candidate selection algorithm to the implementation layer.
- An implementation may use any selection strategy (e.g., first-suitable, round-robin, random, or least-recently-assigned), provided the choice is drawn strictly from the valid candidate set $\mathcal{C}(T)$.
- Routing correctness is established entirely by Stage 1; candidate selection order is purely an operational heuristic.

### 10.7 Worker Eligibility Semantics
- `accepting_new_work = true`: The worker session is eligible to be considered for candidate matching. It is **not** a guarantee that the worker will accept an offer.
- `accepting_new_work = false`: The worker session is ineligible for new candidate matching (due to draining, graceful shutdown, maintenance, or local policy). It does **not** imply full capacity saturation.
- Advanced capacity slots, concurrency limits, and load scores are explicitly deferred from V1.

### 10.8 No-Worker Semantics & Passive Backpressure
- If no eligible, compatible worker session exists for a `RUNNABLE` task ($\mathcal{C}(T) = \emptyset$):
  - The `TaskExecution` **remains `RUNNABLE`**.
  - No `ExecutionAttempt` is created; zero retry budget is consumed.
  - The absence of a worker is **not** a task execution failure. The task waits passively until a compatible worker registers or becomes eligible.
  - This provides natural passive work accumulation without requiring complex admission-control or queue-bounding systems.
- If a task requires an Activity Type that has never been registered, it remains `RUNNABLE`. Workflow execution does not abort; worker availability is dynamic.

### 10.9 Rejection of Routing Wait Timeouts
- V1 introduces **no mandatory routing or queue wait timeout**. A task may wait in `RUNNABLE` indefinitely.
- Excessive wait times are surfaced observationally as operational metrics via [ADR-016](00-architecture-decision-register.md#L221) (e.g., `runnable_wait_age_seconds`) rather than forcing automatic lifecycle failures.

### 10.10 Pre-Ownership Failures & Stale Candidates
- If a candidate worker declines an offer, if network transport fails before ownership commit, or if candidate selection is interrupted:
  - The candidate match is discarded.
  - The task remains `RUNNABLE` and is eligible for future routing.
  - Zero attempts are created; retry budget is unaffected.

### 10.11 Ownership Commit Revalidation Contract
At [ADR-008](adr-008-worker-coordination-and-liveness-model.md) Phase 2 (Ownership Commit), the control plane must revalidate that:
1. `TaskExecution.status == RUNNABLE`.
2. `WorkflowExecution.status == RUNNING` (workflow permits progression).
3. No active `ExecutionAttempt` already exists for this task.
4. Candidate `WorkerSessionId` is currently recognized as live in the registry.
5. Candidate `WorkerSessionId` has `accepting_new_work == true`.
6. Candidate `WorkerSessionId` still advertises the canonical Activity Type.

If any check fails, ownership commit aborts, the candidate match is discarded, and the task remains `RUNNABLE`.

### 10.12 Atomicity & Post-Commit Boundaries
- **ADR-013 Scope**: Atomically guarantees single-winner execution for the durable transition of `TaskExecution` into `RUNNING` and creation of `ExecutionAttempt` in `CLAIMED`.
- **Ephemeral Registry Boundary**: Ephemeral worker registry metadata is not required to reside in the same physical transactional store as durable task state.
- **Post-Commit Boundary**: Single-winner commit does **not** guarantee that the remote worker remains live after commit. If the worker crashes immediately following commit, routing responsibility is complete; [ADR-008](adr-008-worker-coordination-and-liveness-model.md) worker-loss semantics govern the newly created attempt.

### 10.13 Retry Routing Semantics
- When a task transitions `RETRY_WAIT` $\to$ `RUNNABLE` ([ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md)), it enters the routing pipeline under the exact same contract as a first-time task.
- V1 enforces no forced anti-affinity, blacklisting, or failure-aware routing. (If the prior attempt failed due to worker loss, that dead session is naturally ineligible because it is no longer live).

### 10.14 Zero Durable Routing State
- Routing owns **zero correctness-critical durable state**.
- It maintains no persistent queues, cursors, reservation tables, or affinity records.
- Any routing caches or selection counters are purely ephemeral optimizations.

---

## 11. Decision Rationale

1. **Isolation of Routing from Execution Authority**:
   Treating routing decisions as advisory and ephemeral guarantees that network failures, worker offer rejections, or scheduler stalls never inadvertently burn task retry budgets or create zombie attempts.
2. **Symmetric Abstraction for Push and Pull**:
   Modeling routing as a symmetric matching predicate $\text{Match}(T, W)$ ensures that NexusFlow can support both push-based orchestration and pull-based worker architectures without redesigning core domain logic.
3. **Robustness Under Worker Churn**:
   Revalidating preconditions at Phase 2 Ownership Commit allows routing to operate over dynamic, changing worker registries without requiring complex multi-entity distributed transactions during candidate selection.
4. **Simplicity and Extensibility**:
   Separating the strict capability filter from the selection policy keeps the V1 routing core simple and robust, while leaving future extensions (load balancing, locality, priority) cleanly isolated to Stage 2.

---

## 12. Tradeoffs

*   **What is Gained**:
    *   Strict protection of task retry budgets against routing and transport disruptions.
    *   Symmetric compatibility with both push and pull worker models.
    *   Zero durable routing state, simplifying crash recovery.
    *   Passive, natural backpressure when worker capacity is saturated or absent.
*   **What is Sacrificed**:
    *   No load-aware balancing in V1: Tasks are matched based on capability and eligibility, not CPU/memory utilization.
    *   No task prioritization: All `RUNNABLE` tasks of the same Activity Type are considered equally eligible.
    *   Potential routing starvation: If an Activity Type has no registered workers, tasks will wait indefinitely unless caught by operational alerting.

---

## 13. Consequences

### 13.1 Upstream & Downstream Impact
*   **ADR-005 (Scheduler)**: Focuses strictly on DAG dependency satisfaction; relies on ADR-009 to match `RUNNABLE` tasks with capable workers.
*   **ADR-007 (Task Lifecycle)**: Preserves the invariant that attempts are created only upon authoritative worker ownership, not during routing.
*   **ADR-008 (Worker Coordination)**: Consumes Candidate Matches emitted by ADR-009 to execute Phase 2 Ownership Commit; supplies worker capability and liveness metadata.
*   **ADR-011 (Persistence)**: Requires no routing-specific tables or state representations.
*   **ADR-012 (Recovery)**: Requires no routing recovery scan; routing naturally rematches recovered `RUNNABLE` tasks against reconstructed worker registries.
*   **ADR-013 (Concurrency)**: Enforces single-winner execution during Phase 2 Ownership Commit.
*   **ADR-016 (Observability)**: Monitors runnable queue age, routing latency, and unmatched activity types.
*   **ADR-019 (Modularity)**: Confirms routing is a logical component within the orchestrator, not a separate microservice.
*   **ADR-020 (Technology Selection)**: Freedom to implement physical dispatch via HTTP long-polling, direct RPC, or message brokers without violating domain routing.

---

## 14. Failure Modes

| Failure Mode | Direct Consequence | Architectural Mitigation | Owning ADR |
| :--- | :--- | :--- | :--- |
| **No Workers Registered** | Task cannot be matched. | Task remains `RUNNABLE`; zero attempts; retry budget unaffected. | ADR-009, ADR-007 |
| **No Worker Supports Activity Type** | Task cannot be matched. | Task remains `RUNNABLE`; waits for compatible worker deployment. | ADR-009 |
| **Compatible Worker is Draining** | Worker has `accepting_new_work=false`.| Excluded from candidate set; task waits for another eligible worker. | ADR-009, ADR-008 |
| **Worker Dies Before Ownership Commit**| Candidate becomes invalid before commit. | Commit revalidation detects loss; commit aborts; task stays `RUNNABLE`. | ADR-009, ADR-013 |
| **Worker Rejects Candidate Offer** | Worker declines offer pre-commit. | Candidate match discarded; task remains `RUNNABLE` for next cycle. | ADR-009, ADR-008 |
| **Two Routers Select Same Task** | Concurrent candidate selection. | Single-winner commit ensures only one wins; losing offer aborts harmlessly. | ADR-009, ADR-013 |
| **Workflow Cancels During Routing** | Workflow enters `CANCELLING` mid-offer. | Commit revalidation aborts ownership; task transitions to `CANCELLED`. | ADR-009, ADR-006 |
| **Worker Dies Immediately After Commit**| Worker crashes right after ownership. | Routing is complete; ADR-008 worker-loss semantics govern the attempt. | ADR-009, ADR-008 |
| **Orchestrator Crashes During Routing** | In-flight candidate match lost. | Zero durable state lost; task recovered as `RUNNABLE` and rematched. | ADR-009, ADR-012 |
| **Rare Activity Type Starvation** | Task waits indefinitely for missing worker. | Surfaced via operational telemetry (`runnable_wait_age_seconds`). | ADR-009, ADR-016 |

---

## 15. Debugging

Diagnostic observability is maintained by correlating:
* `task_execution_id`: Logical task identifier.
* `activity_type`: Declared Activity Type identity.
* `eligible_candidate_count`: Number of workers in $\mathcal{C}(T)$ at evaluation time.
* `selected_worker_session_id`: Candidate worker chosen by selection policy.
* `revalidation_failure_reason`: Reason for commit abort (e.g., worker lost, not accepting work, task state changed).
* `runnable_since`: Timestamp when task entered `RUNNABLE`.

---

## 16. Testing

The following unit and integration test suites must be developed to validate ADR-009:
*   **Canonical Activity Type Matching Tests**: Assert exact equality matching and verify rejection of wildcards, prefixes, or case variations.
*   **Eligibility Filter Tests**: Assert that workers with `is_live == false` or `accepting_new_work == false` are strictly excluded from candidate sets.
*   **No-Worker Behavior Tests**: Verify that tasks with zero eligible workers remain `RUNNABLE` with zero attempts created.
*   **Advisory Candidate Tests**: Assert that candidate selection mutates neither `TaskExecution` nor `ExecutionAttempt` tables.
*   **Pre-Commit Failure Tests**: Simulate worker rejection or network transport drops and verify that the task remains `RUNNABLE`.
*   **Ownership Revalidation Tests**: Simulate worker crash or workflow cancellation occurring between Stage 2 selection and Ownership Commit; assert that commit aborts cleanly.
*   **Post-Commit Worker Loss Tests**: Verify that worker death immediately after commit transitions the attempt to `FAILED(cause=WORKER_LOST)` via ADR-008 without corrupting routing.
*   **Push and Pull Symmetry Tests**: Validate that identical candidate matches are produced regardless of whether the query begins from a task or a worker.
*   **Retry Routing Tests**: Assert that retried tasks (`RETRY_WAIT` $\to$ `RUNNABLE`) route normally under the standard matching predicate.

---

## 17. Operational Considerations
*   **Monitoring Runnable Wait Age**: Operators must monitor `runnable_wait_age_seconds` via [ADR-016](00-architecture-decision-register.md#L221). A growing runnable queue for a specific Activity Type indicates a worker capacity deficit or missing worker deployment.
*   **Worker Draining Visibility**: Setting `accepting_new_work = false` allows operators to drain worker nodes gracefully without causing routing errors or task failures.
*   **Zero Routing State Overhead**: Operators can restart or upgrade orchestrator instances without worrying about migrating routing queues or active reservation locks.

---

## 18. Maintenance
*   **Preserving the Ownership Boundary**: Developers must ensure that future routing optimizations (e.g., caching, pre-fetching) never bypass the Phase 2 Ownership Commit revalidation checklist.
*   **Extending the Selection Strategy**: New candidate selection algorithms (e.g., least-loaded, round-robin) must be implemented strictly within Stage 2, preserving Stage 1 compatibility invariants.

---

## 19. Future Evolution
*   **V2 Worker Concurrency & Capacity Slots**: Incorporating fine-grained worker slot quotas and concurrent attempt limits into Stage 1 eligibility filtering.
*   **V2 Dynamic Load Balancing**: Implementing least-loaded or weighted candidate selection in Stage 2 based on real-time worker telemetry.
*   **V2 Task Priorities & FIFO Ordering**: Introducing priority-aware candidate selection for workflows with strict SLA requirements.
*   **V2 Locality & Affinity Routing**: Adding data-locality constraints, worker affinity, and retry anti-affinity into Stage 1 filtering.
*   **V2 Dedicated Worker Pools**: Grouping worker sessions into explicit logical pools or queue partitions in coordination with [ADR-020](00-architecture-decision-register.md#L223).

---

## 20. Rejected Alternatives

*   **Broadcast Dispatch**: Rejected because broadcasting tasks to all workers creates massive network amplification and thundering-herd contention during ownership commit.
*   **Mandatory Queue-Per-Activity-Type**: Rejected because coupling routing to message broker topology leaks transport choices into the domain core before [ADR-020](00-architecture-decision-register.md#L223).
*   **Mandatory Round-Robin Selection in V1**: Rejected as an architectural requirement because routing correctness does not depend on selection ordering, and round-robin breaks push/pull symmetry.
*   **Static Hash / Affinity Routing**: Rejected because static hashing creates severe task remapping churn during worker restarts and ignores worker draining status.
*   **Mandatory Routing Wait Timeouts**: Rejected because tasks may legitimately wait in `RUNNABLE` during worker pool scaling; premature timeouts cause false workflow failures.
*   **Implicit Generic Fallback Workers**: Rejected because allowing generic catch-all workers to execute unadvertised Activity Types breaks capability isolation and causes runtime execution errors.
*   **Durable Routing Reservations**: Rejected because tracking ephemeral candidate reservations in persistent storage creates unnecessary database overhead and complex cleanup logic.

---

## 21. Decision Evolution
*   *2026-09-05 (Architectural Review & Refinement)*:
    *   Established two-stage capability matching architecture (Eligibility Filter + Candidate Selection).
    *   Formally defined the symmetric matching predicate $\text{Match}(T, W)$ supporting both push and pull.
    *   Decoupled routing from specific selection algorithms, rejecting mandatory round-robin in V1.
    *   Enforced exact canonical Activity Type identity equality, rejecting wildcards and generic fallbacks.
    *   Clarified that `accepting_new_work` represents eligibility, not a capacity saturation guarantee.
    *   Defined the Ownership Commit revalidation checklist and established zero durable routing state.

---

## 22. Common Misconceptions

*   **Misconception: "Routing assigns authoritative ownership of a task to a worker."**
    *   *Reality*: Routing only identifies an advisory Candidate Match. Authoritative ownership is established strictly during ADR-008 Phase 2 Ownership Commit under atomic concurrency control ([ADR-013](00-architecture-decision-register.md#L212)).
*   **Misconception: "If no worker supports an Activity Type, the workflow fails immediately."**
    *   *Reality*: The task remains in `RUNNABLE`. Worker availability is dynamic; workers supporting that Activity Type may register later.
*   **Misconception: "Routing must use message broker queues."**
    *   *Reality*: Routing is a logical capability filter. It can be implemented over direct HTTP polling, gRPC streams, in-memory lists, or message queues ([ADR-020](00-architecture-decision-register.md#L223)).
*   **Misconception: "NexusFlow guarantees FIFO fairness across runnable tasks."**
    *   *Reality*: V1 provides no FIFO fairness guarantees. Selection among eligible runnable tasks is governed by the pluggable candidate selection policy.

---

## 23. Open Questions
*   **Non-blocking downstream decisions**:
    *   Selection of physical transport protocols and broker topologies ([ADR-020](00-architecture-decision-register.md#L223)).
    *   Specific database indexing strategies for querying `RUNNABLE` tasks ([ADR-011](00-architecture-decision-register.md#L210)).
    *   Default candidate selection algorithm for initial implementation (Implementation choice).
    *   Advanced capacity quotas and priority scheduling (V2 Future).

---

## 24. Interview Discussion

*   **Q: Why separate task routing from task scheduling?**
    *   *A*: Task scheduling (ADR-005) is concerned with workflow graph semantics: evaluating when dependencies are satisfied and when a task is logically ready to run. Task routing (ADR-009) is concerned with physical resource matching: evaluating which worker node has the capabilities to execute that task. Conflating them tightly couples DAG traversal logic to worker registration state, making the engine brittle and difficult to test.
*   **Q: Why doesn't candidate selection create an ExecutionAttempt?**
    *   *A*: Routing is an advisory, asynchronous operation. If an attempt were created during candidate selection, any subsequent network drop, worker rejection, or control-plane restart prior to actual worker acceptance would count as a failed attempt, burning the user's retry budget. Creating the attempt strictly at Phase 2 Ownership Commit ensures that retry budgets are consumed only when worker execution ownership is guaranteed.
*   **Q: How does NexusFlow maintain symmetry between push and pull models?**
    *   *A*: NexusFlow defines routing as a symmetric mathematical relation: $\text{Match}(T, W)$. In a push model, the engine queries for workers matching a runnable task. In a pull model, the engine queries for runnable tasks matching a polling worker. Because both models execute the exact same compatibility filter and ownership commit boundary, the underlying domain logic remains identical regardless of transport direction.
*   **Q: What happens if a worker dies after being selected as a candidate?**
    *   *A*: Because candidate selection is advisory and non-authoritative, nothing breaks. When the engine attempts Phase 2 Ownership Commit, the revalidation checklist detects that the worker is no longer live in the registry. Ownership commit aborts cleanly, the task remains `RUNNABLE`, and the router selects a new candidate on the next cycle.

---

## 25. References
*   [ADR-001: Internal Workflow Specification](adr-001-internal-workflow-specification.md)
*   [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md)
*   [ADR-005: Workflow Task Scheduling & Dispatch Architecture](adr-005-workflow-task-scheduling-and-dispatch-architecture.md)
*   [ADR-007: Task Execution Lifecycle & Attempt Model](adr-007-task-execution-lifecycle-and-attempt-model.md)
*   [ADR-008: Worker Coordination & Liveness Model](adr-008-worker-coordination-and-liveness-model.md)
*   [00-Architecture Decision Register](00-architecture-decision-register.md)

---

## 26. Traceability

| Artifact / Requirement | Addressed in ADR-009 |
| :--- | :--- |
| **Capability 3** | Bridges scheduling progression with worker capability matching. |
| **ADR-005 Scheduler** | Consumes `RUNNABLE` tasks without modifying DAG dependency evaluation. |
| **ADR-007 Task Lifecycle** | Preserves attempt creation boundary; does not consume retry budget during routing. |
| **ADR-008 Worker Coordination** | Implements Phase 1 Candidate Matching; respects `WorkerSessionId` and liveness. |
| **ADR-011 Persistence** | Confirms zero durable routing state; requires no schema additions. |
| **ADR-012 Recovery** | Reconstructs routing naturally from `RUNNABLE` tasks and surviving workers. |
| **ADR-013 Concurrency** | Defines revalidation conditions protected by single-winner ownership commit. |
| **ADR-016 Observability** | Emits routing facts and exposes `runnable_wait_age` monitoring. |

---

## 27. Decision Validation Checklist

*   [x] **Routing begins only for RUNNABLE work?** Yes (Section 10.2).
*   [x] **Canonical Activity Type match defined?** Yes, exact identity equality (Section 10.3).
*   [x] **No wildcard/subtyping/fallback?** Yes, explicitly rejected (Section 10.3, 20).
*   [x] **Worker must be live?** Yes (Section 10.2).
*   [x] **Worker must accept new work?** Yes (Section 10.2).
*   [x] **accepting_new_work not capacity guarantee?** Yes, defined as eligibility (Section 10.7).
*   [x] **Push/pull symmetry preserved?** Yes, symmetric relation $\text{Match}(T, W)$ (Section 10.4).
*   [x] **Candidate selection non-authoritative?** Yes (Section 10.5).
*   [x] **No concrete selection algorithm frozen?** Yes, pluggable policy (Section 10.6).
*   [x] **No fairness guarantee?** Yes, operational policy only (Section 10.6, 20).
*   [x] **Deterministic eligibility only?** Yes (Section 10.2, 10.6).
*   [x] **No-worker leaves RUNNABLE?** Yes (Section 10.8).
*   [x] **Unsupported type does not cause task failure?** Yes (Section 10.8).
*   [x] **No routing wait timeout?** Yes, rejected for V1 (Section 10.9).
*   [x] **Routing creates no Attempt?** Yes (Section 10.5).
*   [x] **Routing consumes no retry budget?** Yes (Section 10.5, 10.8).
*   [x] **Worker rejection pre-commit harmless?** Yes (Section 10.10).
*   [x] **Stale candidate carries no authority?** Yes (Section 10.10).
*   [x] **Commit revalidates conditions?** Yes, 6-point checklist (Section 10.11).
*   [x] **Durable state atomicity boundary belongs ADR-013?** Yes (Section 10.12).
*   [x] **Worker liveness after commit belongs ADR-008?** Yes (Section 10.12).
*   [x] **Retry follows same routing contract?** Yes (Section 10.13).
*   [x] **No mandatory capacity model?** Yes (Section 10.7, 20).
*   [x] **No affinity/priority/locality?** Yes, deferred to V2 (Section 7, 19).
*   [x] **No routing reservation?** Yes (Section 10.5).
*   [x] **No correctness-critical durable routing state?** Yes (Section 10.14).
*   [x] **Recovery state belongs ADR-012?** Yes (Section 10.14, 13.1).
*   [x] **No queue/broker topology selected?** Yes, deferred to ADR-020 (Section 7, 10.4).
*   [x] **No dedicated routing service required?** Yes (Section 7, 13.1).
*   [x] **No fake metrics/production claims?** Yes, verified throughout document.
