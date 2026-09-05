# ADR-004 — Workflow Validation Strategy

*   **Status**: Approved
*   **Last Updated**: 2026-09-05
*   **Deciders**: Project Owner
*   **Domain**: Foundation (Definition & Semantic Verification)
*   **Criticality**: Critical
*   **Relationships**:
    *   **Depends On**: [ADR-001: Internal Workflow Specification](adr-001-internal-workflow-specification.md), [ADR-002: Workflow Definition Parsing Strategy](adr-002-workflow-definition-parsing-strategy.md), [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md)
    *   **Enables**: [ADR-005: Workflow Task Scheduling & Dispatch Architecture](00-architecture-decision-register.md#L200), [ADR-006: Workflow Execution State Machine](00-architecture-decision-register.md#L201)
    *   **Related To**: [ADR-010: Workflow Data Flow & Parameter Passing](00-architecture-decision-register.md#L209), [ADR-015: HTTP & REST API Layer](00-architecture-decision-register.md#L214), [ADR-021: System Testing Strategy](00-architecture-decision-register.md#L220)

---

## 1. Purpose
This document defines the semantic validation architecture for NexusFlow workflow definitions. It establishes how candidate workflow semantics and pre-validation dependency projections are deterministically verified, accepted, or rejected prior to execution eligibility. It guarantees that invalid definitions are caught at definition boundaries, preserving a strict, non-mutating verification model decoupled from volatile runtime states and scheduler execution loops.

---

## 2. Context
NexusFlow separates workflow ingestion into progressive stages:
1. **Authoring & Parsing ([ADR-002](adr-002-workflow-definition-parsing-strategy.md))**: Ingests external authoring formats (e.g., YAML), enforces syntax and structural shapes, applies semantics-preserving normalization, and constructs a Candidate Internal Workflow Specification (Candidate IWS) alongside optional source provenance mapping.
2. **Topology Projection ([ADR-003](adr-003-canonical-workflow-graph-representation.md))**: Extracts dependency declarations into a pre-validation `Dependency Projection`, which may temporarily contain unresolved references, self-dependencies, duplicate semantic edges, or directed cycles.
3. **Execution & Scheduling ([ADR-005](00-architecture-decision-register.md#L200) / [ADR-006](00-architecture-decision-register.md#L201))**: Requires a structurally sound, acyclic, and trustworthy workflow model (`Validated IWS` and `Canonical Workflow Graph`) to manage task eligibility, state transitions, and completion aggregation.

Without a dedicated semantic validation strategy, responsibility boundaries erode:
* Parsing must either guess at graph validity or leak cyclic graph traversal into syntax parsing.
* Schedulers must dynamically handle dangling task references, self-dependencies, or deadlocking cycles in real-time execution loops.
* Diagnostic feedback becomes opaque, providing runtime panics or generic exceptions instead of actionable, source-correlated errors.

ADR-004 defines the semantic acceptance boundary governing the promotion of candidate definitions into execution-eligible artifacts.

---

## 3. Problem Statement
Given a structurally parseable `Candidate IWS` and its corresponding `Dependency Projection`, how should NexusFlow deterministically determine that the workflow represents a semantically valid definition, while producing actionable diagnostics and keeping validation strictly decoupled from parsing, graph representation, scheduling, execution state, worker availability, persistence, and API transport?

---

## 4. Requirements Covered
*   **System Invariant (KT-8)**: "A workflow definition must be an acyclic directed graph (DAG)."
*   **System Invariant Boundary (KT-8)**: ADR-004 validates the static existence and acyclic structure of declared dependencies; runtime satisfaction of those dependencies ("No task can be executed until all its declared dependencies have successfully completed") is owned by [ADR-005](00-architecture-decision-register.md#L200) and [ADR-006](00-architecture-decision-register.md#L201).
*   **Core Principle**: Correctness Over Performance.
*   **Core Principle**: Explicit Behaviour Over Implicit Behaviour.
*   **Core Principle**: Technology Independence Before Implementation.
*   **Core Principle**: Clear Ownership and Separation of Responsibilities.
*   **Core Principle**: Every Abstraction Must Solve a Real Problem.

---

## 5. Constraints
*   **Non-Mutating Verification**: Validation must act strictly as a verifier. It must not alter, repair, synthesize, or normalize workflow semantics, task definitions, or dependency edges.
*   **Single-Developer Feasibility**: The architecture must remain simple, robust, and maintainable without introducing enterprise rule engines, validation DSLs, dynamic plugin loaders, or distributed validator clusters.
*   **Resource Safety**: Diagnostic collection must be bounded by an implementation-defined safety limit to prevent resource exhaustion from pathological inputs.
*   **Runtime Independence**: Definition validity must be decoupled from volatile runtime conditions (e.g., worker pool availability, queue depth, or infrastructure health).
*   **Algorithmic Efficiency**: Graph topology validation must be achievable with linear time complexity $O(V + E)$ relative to task count ($V$) and dependency edges ($E$).

---

## 6. Goals
*   Establish the semantic acceptance boundary promoting `Candidate IWS` and `Dependency Projection` to `Validated IWS` and `Canonical Workflow Graph`.
*   Enforce a dependency-aware, phased error accumulation strategy that gathers independent diagnostics while preventing algorithmic panics or cascading errors across dependent checks.
*   Enforce domain invariants: reject empty workflows, duplicate task identities, unresolved dependency targets, duplicate semantic dependencies, self-dependencies, and directed cycles.
*   Permit valid DAG patterns without artificial constraints: allow multiple roots, multiple leaves, and disconnected DAG components without requiring synthetic START/END nodes.
*   Define a structured internal diagnostic contract providing actionable feedback (codes, messages, paths, cycle paths, and detached source provenance).
*   Guarantee deterministic validation outcomes and deterministic diagnostic ordering for equivalent semantic inputs.

---

## 7. Non-Goals
*   Defining concrete cycle-detection algorithms or internal graph traversal data structures (e.g., mandating 3-color DFS vs. Kahn's algorithm; deferred to implementation).
*   Deciding Activity Type catalog existence policies or worker capability verification (deferred to Activity Registration / Worker Coordination architecture).
*   Defining task eligibility rules, ready queue mechanics, dispatching loops, or runtime remaining-dependency counters (deferred to [ADR-005](00-architecture-decision-register.md#L200)).
*   Defining workflow execution state machines, completion aggregation, cancellation logic, or terminal state transitions (deferred to [ADR-006](00-architecture-decision-register.md#L201)).
*   Defining task attempt lifecycles, execution retry tracking, or worker heartbeats (deferred to [ADR-007](00-architecture-decision-register.md#L202)).
*   Inventing data-flow dependency rules or parameter schema matching (deferred to [ADR-010](00-architecture-decision-register.md#L209)).
*   Designing HTTP status codes, REST error envelopes, or API transport serialization (deferred to [ADR-015](00-architecture-decision-register.md#L214)).
*   Designing database schemas, validation status persistence flags, or version migration strategies (deferred to [ADR-011](00-architecture-decision-register.md#L210), [ADR-012](00-architecture-decision-register.md#L211), and [ADR-024](00-architecture-decision-register.md#L223)).

---

## 8. Candidate Solutions

### Alternative A: Minimal Global Fail-Fast Validator
A sequential validation pass that immediately halts and returns upon encountering the very first validation error.
*   *Mechanism*: Evaluates checks sequentially; throws or returns on the first failed assertion.
*   *Pros*: Extremely simple to implement; minimal memory overhead; eliminates concerns about dependent validator cascading.
*   *Cons*: Degraded developer experience; requires authors to fix and re-submit repeatedly to discover multiple errors; poor diagnostic utility.

### Alternative B: Unrestricted Collect-All Validator
An open validation runner where every check runs independently across the definition, accumulating all failures into a global list.
*   *Mechanism*: Runs identity checks, reference checks, cycle detection, and domain checks unconditionally.
*   *Pros*: Maximizes error discovery in a single pass.
*   *Cons*: Highly prone to runtime crashes (e.g., cycle detectors attempting to traverse missing task nodes or dereferencing null pointers); floods diagnostics with confusing secondary/phantom errors caused by root-level invariant violations.

### Alternative C: Dependency-Aware Phased Semantic Validation Pipeline [Selected]
A phased validation model grouped by semantic dependencies. Within a phase, independent checks accumulate errors safely. Between phases, execution gates prevent running dependent checks whose structural prerequisites have failed.
*   *Mechanism*: Organizes validation into explicit, dependency-ordered groups: Entity/Identity $\to$ Reference Integrity $\to$ Local Dependency Semantics $\to$ Graph-Wide Topology $\to$ Domain Field Semantics. Accumulation is bounded by a safety cap.
*   *Pros*: Maximizes actionable diagnostics safely; prevents cascading errors and algorithmic traversal panics; clean separation of concerns; deterministic behavior.
*   *Cons*: Requires explicit definition of phase gates and prerequisites.

### Alternative D: Distributed Validation Embedded Across Parser, Graph, and Scheduler
Validation logic is scattered across existing components: parsing validates identity, the graph validates topology during construction, and the scheduler validates references at dispatch.
*   *Mechanism*: No centralized validation boundary; checks happen opportunistically.
*   *Pros*: Avoids a dedicated validation step.
*   *Cons*: Completely violates separation of responsibilities; leaks runtime state into parsing and vice-versa; introduces partial failures during execution loops; impossible to achieve deterministic upfront validation.

### Alternative E: Generic Rule/Plugin Validation Framework
An enterprise-style rule engine where individual validation rules implement a common interface and are dynamically discovered, ordered, and executed via a dependency graph or plugin registry.
*   *Mechanism*: Decoupled validator plugins registered into a runtime rule engine with dynamic dependency resolution.
*   *Pros*: Arbitrarily extensible; facilitates third-party validation plugins.
*   *Cons*: Severe overengineering for a V1 single-developer engine; high cognitive and abstraction overhead; complex debugging; risk of dynamic ordering nondeterminism.

---

## 9. Detailed Evaluation of Every Candidate

| Evaluation Criteria | Alt A: Global Fail-Fast | Alt B: Unrestricted Collect-All | Alt C: Phased Pipeline (Selected) | Alt D: Distributed Validation | Alt E: Generic Rule Engine |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Correctness & Safety** | High | Low (crashes on malformed graphs) | **Highest** (gated preconditions) | Low (leaks invalid states) | Medium (complex engine interactions) |
| **Diagnostic Quality** | Poor (1 error at a time) | High but noisy (cascading phantoms) | **Optimal** (relevant, accurate errors) | Poor (scattered, runtime panics) | High |
| **Implementation Simplicity** | Highest | High | **Balanced & High** (V1 appropriate) | Low (tangled boundaries) | Lowest (high abstraction bloat) |
| **Determinism** | High | Medium | **High** (stable ordering & gates) | Lowest (timing/path dependent) | Medium (plugin order hazards) |
| **Maintainability** | High | Low (fragile checks) | **High** (clear grouping & gates) | Lowest (logic fragmentation) | Low (indirection bloat) |
| **Solo-Developer Feasibility**| High | High | **High** | Medium | Low |

---

## 10. Decision
NexusFlow adopts **Alternative C: Dependency-Aware Phased Semantic Validation Pipeline** as the architectural strategy for workflow definition acceptance.

### 10.1 Core Conceptual Lifecycle
Validation acts as an atomic semantic boundary between candidate representations and runtime-eligible models:

```
        Candidate IWS (ADR-002)
                   +
     Dependency Projection (ADR-003)
                   +
      Optional Source Provenance
                   │
                   ▼
┌──────────────────────────────────────┐
│  ADR-004 Semantic Validation Engine  │
│                                      │
│  Group 1: Entity & Identity          │
│           │ (Gate 1)                 │
│  Group 2: Reference Integrity        │
│           │ (Gate 2)                 │
│  Group 3: Local Dependency Semantics │
│           │ (Gate 3)                 │
│  Group 4: Graph-Wide Topology (DAG)  │
│           │ (Gate 4)                 │
│  Group 5: Domain Field Semantics     │
└──────────────────┬───────────────────┘
                   │
         ┌─────────┴─────────┐
         ▼                   ▼
   [Any Errors]        [Zero Errors]
         │                   │
         ▼                   ▼
 Validation Failure    Promotion to Execution-Eligible:
 Returns Structured    - Validated IWS
 Diagnostics Only      - Canonical Workflow Graph
                       - Scheduler Access Permitted
```

### 10.2 Strict Verifier Semantics (No Silent Repair)
ADR-004 is strictly a verifier, not a mutation or normalization engine. It **MUST NOT** silently:
* Deduplicate semantic dependencies (e.g., `[A, A]` is rejected, not collapsed).
* Remove directed cycles or self-dependencies.
* Invent missing task definitions or create phantom nodes.
* Rewrite semantic identifiers or substitute Activity Types.
* Insert synthetic START or END nodes.
* Alter retry, timeout, or execution policy semantics.

### 10.3 Non-Mutating Contract & Lifecycle Promotion
Validation does not mutate `Candidate IWS` or `Dependency Projection`. Promotion to `Validated IWS` and `Canonical Workflow Graph` represents a lifecycle state transition rather than requiring a physical memory deep-copy. If validation fails, no execution-eligible artifacts are produced, and execution components are prevented from consuming the definition.

### 10.4 Semantic Validation Rules & Topology Acceptance
1. **Empty Workflow Rejection**: A definition must contain at least one Task Definition ($|V| \ge 1$). Empty definitions (`tasks: []`) are rejected as having no executable semantics.
2. **Task Definition Identity Uniqueness**: All canonical `TaskDefinitionId` values must be non-empty and mutually unique within the workflow scope. Overwriting, merging, or last-definition-wins semantics are strictly prohibited.
3. **Reference Integrity**: Every dependency target declared in `depends_on` must reference an existing `TaskDefinitionId` within the workflow definition. Missing targets are rejected; phantom nodes are never created.
4. **No Duplicate Semantic Dependencies**: Repeated declarations of the same dependency target (e.g., `depends_on: [A, A]`) are rejected as semantic ambiguities. (Distinguished from duplicate syntax keys, which are rejected in ADR-002).
5. **No Self-Dependencies**: A task depending on itself (`A -> A`) is strictly invalid and must yield a dedicated `TASK_SELF_DEPENDENCY` diagnostic prior to general cycle detection.
6. **Acyclic Directed Topology (DAG)**: Any directed cycle ($A \to B \to A$ or $A \to B \to \dots \to A$) is strictly invalid.
7. **Multiple Roots Permitted**: Workflows may contain multiple tasks with zero incoming dependencies. No synthetic START task is required.
8. **Multiple Leaves Permitted**: Workflows may contain multiple tasks with zero outgoing dependencies. No synthetic END task is required.
9. **Disconnected DAG Components Permitted**: Disconnected acyclic components (e.g., $A \to B$ and $C \to D$) are valid. Workflows may declare independent concurrent pipelines sharing a common definition lifecycle.
10. **No Global Single-Root Reachability Requirement**: Reachability from a single designated entry point is not required because NexusFlow definitions do not mandate synthetic entry tasks.

### 10.5 Dependency-Aware Validation Gating
Validation groups are executed using explicit prerequisite gates:
* **Gate 1 (Identity Integrity)**: If empty workflow or duplicate task IDs exist, halt before reference resolution.
* **Gate 2 (Reference Integrity)**: If unresolved dependency targets exist, halt before graph-wide topological traversal.
* **Gate 3 (Local Dependency Integrity)**: If self-dependencies exist, report them and halt before global cycle traversal.
* **Gate 4 (Graph-Wide Topology)**: Traverses the `Dependency Projection` in $O(V + E)$ complexity to verify acyclicity.
* **Independent Domain Checks**: Domain field invariants (e.g., retry count $\ge 0$, positive timeout durations) may execute once entity definitions are established.

### 10.6 Bounded Diagnostic Accumulation & Determinism
* **Safety Bounding**: Diagnostic collection is capped at an implementation-defined threshold (e.g., maximum diagnostic count) to prevent memory exhaustion when validating malicious or pathological definitions.
* **Deterministic Contract**: Equivalent semantic inputs and validation context must yield the exact same validity outcome, identical diagnostic error codes, and deterministic diagnostic ordering (ordered conceptually by Validation Group $\to$ Semantic Path / TaskDefinitionId $\to$ Diagnostic Code).

### 10.7 Definition Validity vs. Runtime Executability
Definition validity is an intrinsic, immutable property of the workflow definition. Runtime executability is a volatile operational state. A definition remains valid even when zero workers are online, queues are congested, or supporting infrastructure is unavailable.

---

## 11. Decision Rationale
1. **Preserving Author Intent via Strict Verification**: Silently fixing author mistakes such as duplicate dependencies, unresolved references, or self-dependencies conceals bugs. If an author typed a duplicate dependency, it often indicates a copy-paste error where another task was intended. Explicit rejection forces clarity.
2. **Preventing Algorithmic Crashes via Gated Phases**: Running cycle detection algorithms (such as DFS or topological sorting) on graphs with unresolved node targets causes missing-key lookups or null-pointer dereferences. Enforcing reference integrity prior to graph traversal guarantees algorithmic safety.
3. **Maximizing Usability Without Noise**: Phased accumulation allows developers to see identity, reference, local dependency, and other safely independent semantic errors in a single validation pass, while safely withholding cycle detection until the graph topology is referentially sound.
4. **Natural Workflow Concurrency via Disconnected DAGs**: Real-world operational workflows frequently execute parallel, independent pipelines (e.g., parallel data ingest and metrics warmup). Requiring connectivity forces boilerplate dummy nodes (`start_node` / `end_node`), complicating definition authoring and runtime monitoring.
5. **Operational Decoupling from Workers**: Allowing worker availability to affect definition validity creates race conditions during deployments: workflows could not be registered unless worker pools were already warm, breaking deployment automation.

---

## 12. Tradeoffs
*   **Upfront Rigor vs. Permissive Ingestion**: We sacrifice lenient, auto-correcting ingestion in favor of strict, unambiguous definition semantics. Authors must explicitly provide clean definitions.
*   **Diagnostic Safety Cap vs. Exhaustive Diagnostic Collection**: On deeply pathological inputs with thousands of errors, capping diagnostics means authors will only see errors up to the safety threshold until initial issues are resolved. This trade-off is necessary to protect engine resources.
*   **Actionable Cycle Paths vs. Minimal Traversal Code**: Extracting and formatting actionable cycle paths ($A \to B \to A$) requires slightly more graph traversal bookkeeping than simply returning a boolean `has_cycle` flag, but provides vastly superior developer experience.

---

## 13. Consequences

### Codebase & Architectural Impact
*   **Explicit Promotion Boundary**: Downstream components ([ADR-005](00-architecture-decision-register.md#L200), [ADR-006](00-architecture-decision-register.md#L201)) can trust that any received definition is a fully validated DAG with unique identities and resolvable references.
*   **Topology Traversal Reuse**: Graph-wide checks operate directly on the `Dependency Projection` produced by ADR-003, avoiding redundant graph construction in ADR-004.
*   **Clean Separation from Transport**: ADR-004 outputs structured semantic diagnostics internally, allowing [ADR-015](00-architecture-decision-register.md#L214) to format them into HTTP REST envelopes without coupling validation to HTTP concerns.

### Performance Impact
*   Semantic validation primarily occurs at workflow-definition lifecycle boundaries and is not part of the high-frequency task execution path. Exact registration, load, recovery, migration, and revalidation behavior is deferred to persistence/recovery/versioning ADRs.
*   Topology checks operate in $O(V + E)$ linear time and space, preventing exponential backtracking or super-linear bottlenecks on large workflow definitions.

---

## 14. Failure Modes

| Failure Mode | Symptom / Consequence | Owning ADR | Mitigation |
| :--- | :--- | :--- | :--- |
| **Duplicate TaskDefinitionId** | Ambiguous graph addressing and lookup collisions | ADR-004 | Gate 1 check: Rejects duplicate canonical IDs with `WF_DUPLICATE_TASK_ID`. |
| **Missing Dependency Target** | Dangling edge; graph traversal null dereference | ADR-004 | Gate 2 check: Verifies all targets exist; rejects with `WF_UNRESOLVED_DEPENDENCY`. |
| **Duplicate Semantic Dependency** | Ambiguous edge declaration | ADR-004 | Rejects `depends_on: [A, A]` with `WF_DUPLICATE_DEPENDENCY`. |
| **Self-Dependency** | Degenerate single-node deadlock | ADR-004 | Gate 3 check: Emits specific `WF_SELF_DEPENDENCY` before cycle traversal. |
| **Multi-Node Cycle** | Workflow deadlock; impossible task readiness | ADR-004 | Gate 4 check: Linear cycle detection emits `WF_CYCLIC_DEPENDENCY` with cycle path. |
| **Empty Workflow** | Non-executable definition registered | ADR-004 | Rejects definitions with zero tasks via `WF_EMPTY_WORKFLOW`. |
| **Diagnostic Flood (DoS)** | Memory exhaustion from pathological definition | ADR-004 | Enforces implementation-defined diagnostic accumulation cap. |
| **Validator Mutation** | Definition semantics altered during validation | ADR-004 | Pure verifier contract; immutability assertions in test suite. |
| **Leaking Unvalidated Graph** | Scheduler executes corrupted or cyclic graph | ADR-004 / ADR-005 | Atomicity rule: Promoted graph returned only when error count is strictly zero. |
| **Validator Internal Crash** | Uncaught exception leaks to caller | ADR-004 | Prerequisite gates reduce invalid-input algorithm failures; unexpected validator failures must fail closed and must never produce execution-eligible artifacts. |
| **Unavailable Provenance** | Diagnostics lack source line/column numbers | ADR-002 / ADR-004 | Graceful degradation: Diagnostic emits semantic entity path if source map is absent. |

---

## 15. Debugging Considerations
*   **Structured Diagnostic Output**: Every validation failure produces a structured diagnostic containing a stable machine-readable code, human-readable description, semantic JSON path, associated task IDs, and optional line/column provenance.
*   **Cycle Path Tracing**: When a cycle is detected, diagnostics report the active cyclic path (e.g., `Task "A" -> Task "B" -> Task "C" -> Task "A"`), allowing immediate visual identification of deadlocks.
*   **Prerequisite Gate Logging**: Debug logs must clearly indicate when downstream validation checks were safely skipped due to upstream prerequisite failures (e.g., "Cycle detection skipped due to 2 unresolved dependency targets").

---

## 16. Testing Considerations
Validation testing must verify the following scenarios (coordinated with [ADR-021](00-architecture-decision-register.md#L220)):
1.  **Valid Workflows**: Single task, sequential linear chain, fan-out, fan-in, diamond DAG, multiple independent roots, multiple independent leaves, disconnected multi-component DAGs.
2.  **Entity Invariants**: Empty workflow rejection, duplicate `TaskDefinitionId` rejection, invalid identifier formats.
3.  **Reference Invariants**: Single and multiple missing dependency targets; verified that graph traversal is safely withheld.
4.  **Local Dependency Invariants**: Self-dependencies ($A \to A$), duplicate semantic dependencies ($A \to [B, B]$).
5.  **Cycle Detection**: Direct 2-node cycle ($A \to B \to A$), multi-node cycle ($A \to B \to C \to A$), disconnected sub-graph cycle ($A \to B$ valid, $C \to D \to C$ cyclic).
6.  **Gating & Accumulation**: Independent errors accumulated in a single pass; dependent checks gated out; diagnostic count cap enforced on pathological inputs.
7.  **Determinism & Non-Mutation**: Identical diagnostics and ordering across repeated test runs; deep-equality assertions verifying input `Candidate IWS` is unmodified.
8.  **Promotion Isolation**: Asserting that failed definitions produce null/empty execution-eligible outputs.

---

## 17. Operational Considerations
*   **Execution-Path Decoupling**: Validation is performed at definition-lifecycle boundaries rather than on the high-frequency execution path. With graph-wide checks designed for $O(V + E)$ complexity, the architecture avoids unnecessary super-linear topology processing. Concrete latency characteristics will be established through benchmarking.
*   **Infrastructure Independence**: Core V1 semantic validation does not depend on volatile runtime infrastructure. Future validation rules may consume explicitly defined, stable definition-time context if required by another ADR; they must not depend on worker availability or transient runtime health.

---

## 18. Maintenance Considerations
*   **Adding Domain Invariants**: New domain-specific invariants (e.g., payload size validation from [ADR-010](00-architecture-decision-register.md#L209)) integrate into Group 5 (Domain Field Semantics) without altering graph topology logic.
*   **No Code Duplication**: Graph-wide validation consumes ADR-003's `Dependency Projection` directly, ensuring graph indexing logic is maintained in exactly one place.

---

## 19. Future Evolution
*   **Advisories & Warnings (V2)**: If product requirements demand warnings (e.g., flagging disconnected components or unusually large fan-outs as potential author oversights), the diagnostic model can support a non-fatal `WARNING` severity without altering the core pass/fail gating.
*   **Cross-Domain Data Flow Validation (ADR-010)**: Validating that task inputs match upstream task outputs can be introduced as an additional validation phase once data-flow semantics are settled.
*   **Definition Version Migration (ADR-024)**: When historical workflow definitions are loaded under updated engine validation rules, migration adapters will run before revalidation.

---

## 20. Rejected Alternatives
*   **Rejected Silent Deduplication of Dependencies**: Discarded because silent repair masks copy-paste authoring errors and violates the principle of explicit behavior.
*   **Rejected Mandatory Graph Connectivity (Synthetic Nodes)**: Discarded because forcing synthetic START/END nodes pollutes the domain model and complicates runtime execution tracking.
*   **Rejected Global Fail-Fast**: Discarded due to unacceptable developer experience and inefficient fix/resubmit debugging cycles.
*   **Rejected Unrestricted Collect-All**: Discarded because running graph traversal on unresolvable references causes runtime crashes and noisy secondary errors.
*   **Rejected Pluggable Validation Rule Engine**: Discarded as unnecessary enterprise abstraction that introduces complexity and overhead for a V1 solo-developer architecture.

---

## 21. Decision Evolution
*   *2026-09-05*: Initial draft establishing phased error accumulation, strict verifier semantics, rejection of empty/cyclic/duplicate structures, and acceptance of multiple roots/leaves/disconnected DAGs.

---

## 22. Common Misconceptions
*   *Misconception*: "Validation should automatically remove duplicate dependencies because sets naturally deduplicate them."
    *   *Correction*: Authoring `depends_on: [A, A]` is an explicit author mistake or ambiguity. Silently deduplicating turns validation into a compiler/mutation pass and hides bugs.
*   *Misconception*: "A disconnected workflow graph is malformed because all tasks must connect to a workflow lifecycle."
    *   *Correction*: Disconnected DAG components share the same workflow instance lifecycle. Forcing artificial connectivity introduces unnecessary synthetic boilerplate nodes.
*   *Misconception*: "If no workers exist for a task's Activity Type, the workflow is invalid."
    *   *Correction*: Definition validity is an immutable property of the specification. Worker capacity is a volatile runtime condition. A workflow remains completely valid even if workers are currently offline.
*   *Misconception*: "Validation runs on every scheduler loop to check if tasks are ready."
    *   *Correction*: Validation is an upfront definition lifecycle gate. Runtime readiness and dependency satisfaction are managed dynamically by the Scheduler ([ADR-005](00-architecture-decision-register.md#L200)).

---

## 23. Open Questions
*   *Activity Type Existence Policy*: Should activity names be validated against a static definition-time catalog during registration, or resolved dynamically at task dispatch? *(Deferred to Activity Registration & Worker Coordination architecture).*
*   *Exact Diagnostic Limit Value*: What concrete threshold should bound accumulated diagnostics in production configurations? *(Deferred to operational configuration and security policy).*

---

## 24. Interview Discussion

### Why is ADR-002 structural validation not enough?
ADR-002 validates syntax, schema field types, unknown keys, and structural container shapes. It ensures that a YAML file is validly structured and normalizable into Candidate IWS. However, ADR-002 does not and should not know about workflow topology or domain semantics: it cannot determine whether task references exist, whether dependencies form deadlocking cycles, or whether business invariants are satisfied across tasks.

### Why separate Candidate IWS from Validated IWS if validation does not mutate the object?
The separation represents a fundamental semantic acceptance and lifecycle boundary. `Candidate IWS` represents an unverified syntactic candidate. `Validated IWS` represents an accepted, execution-eligible definition guaranteed to be free of semantic deadlocks, dangling references, and identity collisions. Schedulers and execution engines can safely consume `Validated IWS` without defensive runtime error-checking.

### Why not silently deduplicate duplicate dependencies?
Silently deduplicating `depends_on: [A, A]` masks human authoring errors (e.g., accidentally writing `task_a` twice instead of `task_a` and `task_b`). Furthermore, it violates the core principle of *Explicit Behaviour Over Implicit Behaviour* and compromises the non-mutating verifier contract.

### Why use phased validation instead of global fail-fast or unrestricted collect-all?
Global fail-fast forces a frustrating "fix-one-error-resubmit" cycle for developers. Unrestricted collect-all attempts to run graph traversal even when task nodes do not exist, leading to internal null-pointer dereferences or flood of meaningless secondary errors. Phased accumulation safely collects all independent errors within a phase while withholding dependent checks until prerequisites are satisfied.

### Why allow disconnected DAG components?
Modern workflows often coordinate independent pipelines under a single business execution (e.g., ingest customer records while simultaneously warming up an search index). Forcing all nodes to connect requires synthetic dummy tasks (`noop_start`, `noop_end`), which clutters definition schemas, increases storage overhead, and creates artificial dependencies.

### Why shouldn't worker availability affect definition validity?
Worker pools are elastic and volatile: workers crash, scale down, restart, or join dynamically. If definition validity depended on worker presence, a workflow valid at 10:00 AM would become invalid at 10:01 AM when a worker restarts. Definition validity must remain an immutable property of the specification.

### Why require O(V + E) complexity for cycle detection without mandating the algorithm?
Linear complexity $O(V + E)$ ensures the validation phase scales safely without exponential latency traps on complex workflows. Mandating the exact algorithm (e.g., 3-color DFS vs. Kahn's algorithm) in an ADR over-constrains implementation; the ADR establishes the complexity invariant and the diagnostic requirement to extract actionable cycle paths, leaving data structure implementation to the engineer.

---

## 25. References
*   [ADR-001: Internal Workflow Specification](adr-001-internal-workflow-specification.md)
*   [ADR-002: Workflow Definition Parsing Strategy](adr-002-workflow-definition-parsing-strategy.md)
*   [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md)
*   [Architecture Decision Register](00-architecture-decision-register.md)
*   Tarjan, R. "Depth-First Search and Linear Graph Algorithms." *SIAM Journal on Computing*, 1972.

---

## 26. Traceability
*   **Depends On**: [ADR-001](adr-001-internal-workflow-specification.md), [ADR-002](adr-002-workflow-definition-parsing-strategy.md), [ADR-003](adr-003-canonical-workflow-graph-representation.md)
*   **Enables**: [ADR-005](00-architecture-decision-register.md#L200), [ADR-006](00-architecture-decision-register.md#L201)
*   **Related To**: [ADR-010](00-architecture-decision-register.md#L209), [ADR-015](00-architecture-decision-register.md#L214), [ADR-021](00-architecture-decision-register.md#L220)

---

## 27. Decision Validation Checklist
*   [x] Is the problem statement decoupled from specific database/broker technologies?
*   [x] Are the functional requirements (FRs) and non-functional requirements (NFRs) traced?
*   [x] Were at least two realistic candidate designs critically evaluated?
*   [x] Are the tradeoffs clear (what are we giving up for simplicity or correctness)?
*   [x] Does the design preserve all mapped system invariants?
*   [x] Does this decision avoid introducing tight coupling between modules?
*   [x] Are the potential failure modes mapped?
*   [N/A] Is there a clear explanation of how this design behaves during a graceful shutdown? *(Justification: Decoupled definition validation logic; graceful shutdown is an execution concern owned by ADR-017).*
*   [x] Are the debugging strategies defined?
*   [N/A] Does the testing strategy explain how to simulate failures and recovery? *(Justification: ADR-004 tests semantic validation failures and promotion isolation; execution crash recovery is owned by ADR-012).*
*   [x] Are the performance limits and resource footprints qualitatively identified?
*   [N/A] Is the future evolution path to HA or multi-tenant deployment explained? *(Justification: Pure in-memory definition verification; clustering and multi-tenancy are deferred to ADR-025).*
*   [x] Can this decision be defended during an SDE-2 engineering review?
