# ADR-003 — Canonical Workflow Graph Representation

*   **Status**: Approved
*   **Last Updated**: 2026-08-30
*   **Deciders**: Project Owner
*   **Domain**: Foundation (Definition & Representation)
*   **Criticality**: Critical
*   **Relationships**:
    *   **Depends On**: [ADR-001: Internal Workflow Specification](adr-001-internal-workflow-specification.md)
    *   **Implements**: Capability 4
    *   **Related To**: [ADR-002: Workflow Definition Parsing Strategy](adr-002-workflow-definition-parsing-strategy.md)

---

## 1. Purpose
This document defines the structural representation, operational boundaries, and derivation lifecycle of the workflow dependency graph within NexusFlow. It establishes how workflow dependency declarations map to an immutable directed topology, separating static workflow structures from mutable runtime scheduler executions.

---

## 2. Context
NexusFlow accepts workflow definitions consisting of task nodes and declared dependencies. While the [Internal Workflow Specification (IWS)](adr-001-internal-workflow-specification.md) acts as the canonical semantic model of a definition, scheduling executions and validating topological soundness require a dedicated graph representation. 

Without a clear graph abstraction, scheduler logic (checking task eligibility and propagating task completions) requires repeatedly traversing IWS structures. This results in $O(V + E)$ full scans of the definition model, leading to performance bottlenecks during high-frequency execution cycles. Furthermore, semantic validators require a graph representation to detect cyclic paths or disconnected components.

---

## 3. Problem Statement
What graph representation should NexusFlow derive from workflow dependency declarations to ensure that dependency traversal, validation, scheduling, and recovery are highly efficient and correct, without duplicating semantic definition state or leaking mutable execution state into the graph nodes?

---

## 4. Requirements Covered
*   **Capability 4**: Workflow execution path determination / dependency traversal.
*   **Core Principle**: Technology Independence Before Implementation.
*   **Core Principle**: Clear Ownership and Separation of Responsibilities.
*   **Core Principle**: Every Abstraction Must Solve a Real Problem.

---

## 5. Constraints
*   **Complete Execution Independence**: The graph structure must not contain execution-specific data, remaining identical regardless of the number of concurrent executions referencing it.
*   **Single-Node V1 Focus**: V1 does not require distributed storage or partitioning of individual graph nodes/edges. The graph is a derived topology whose persistence/materialization strategy is deferred.
*   **Edge Simplicity**: For V1, a dependency edge represents only a structural execution boundary ("Task B depends on Task A"), without speculative extension semantics (like conditional execution branches).

---

## 6. Goals
*   Establish the lifecycle boundary for graph derivation, distinguishing between pre-validation `Dependency Projection` and post-validation `Canonical Workflow Graph` structures.
*   Design a sparse, bidirectional adjacency graph representation that optimizes down-stream completion propagation and task prerequisite checks.
*   Enforce a complete separation of static topology from mutable scheduler executions.
*   Maintain traceable association from graph nodes to canonical Task Definition identity, and from runtime Task Executions/Attempts back to that Task Definition.

---

## 7. Non-Goals
*   Defining cycle detection validation algorithms, self-dependency rejections, or disconnected component acceptance policies (deferred to [ADR-004](00-architecture-decision-register.md#L195)).
*   Defining task scheduling readiness, execution concurrency rules, worker queue dispatches, or scheduling loops (deferred to [ADR-005](00-architecture-decision-register.md#L200)).
*   Selecting the database persistence schema or caching structures for definition storage (deferred to [ADR-011](00-architecture-decision-register.md#L210) & [ADR-012](00-architecture-decision-register.md#L211)).
*   Designing topological visualization export formats or remote registry protocols.

---

## 8. Candidate Solutions

### Alternative A — Directly Traverse IWS Dependency Declarations
No independent graph structure is created. The scheduler and validator query the IWS fields directly during runtime traversal.
*   **Mechanism**: Downstream tasks are found by iterating through all task definitions in the IWS, comparing dependency arrays.

### Alternative B — Forward Adjacency Index Only
The graph tracks only outgoing dependent nodes.
*   **Mechanism**: A map tracking `TaskDefinitionId -> Set<TaskDefinitionId>` representing `dependents[A] = {B, C}`.

### Alternative C — Reverse Adjacency Index Only
The graph tracks only incoming prerequisite nodes.
*   **Mechanism**: A map tracking `TaskDefinitionId -> Set<TaskDefinitionId>` representing `dependencies[D] = {B, C}`.

### Alternative D — Bidirectional Adjacency Indexes [Selected]
The graph maintains both incoming (`dependencies`) and outgoing (`dependents`) adjacency sets.
*   **Mechanism**: Storing bidirectional indexes: `TaskDefinitionId -> Set<TaskDefinitionId>` for both traversal paths.

### Alternative E — Rich Mutable Graph-Node Object Model
Graph nodes are objects that contain the task definitions, execution state flags, and remaining dependency counters.
*   **Mechanism**: Graph nodes hold references to the full task definitions along with mutable runtime states.

### Alternative F — Adjacency Matrix
A 2D array representation tracking connections between all node indices.
*   **Mechanism**: A $V \times V$ matrix where matrix $[A][B] = 1$ denotes a dependency.

---

## 9. Detailed Evaluation of Every Candidate

| Criteria | Alternative A (Traverse IWS) | Alternative B (Forward Only) | Alternative C (Reverse Only) | Alternative D (Bidirectional) [Selected] | Alternative E (Rich Mutable Nodes) | Alternative F (Matrix) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Storage Complexity** | $O(V)$ (No extra graph space). | $O(V + E)$ (Minimal). | $O(V + E)$ (Minimal). | **$O(V + E)$** (Slightly larger index size). | $O(V + E)$ (Highly bloated). | $O(V^2)$ (Severe space penalty). |
| **Eligibility Check** | $O(V)$ lookup scan. | $O(V)$ scan. | **$O(indegree(T))$**. | **$O(indegree(T))$**. | $O(1)$ read from node state. | $O(V)$ check of column. |
| **Completion Propagation** | $O(V)$ scan. | **$O(outdegree(T))$**. | $O(V)$ scan. | **$O(outdegree(T))$**. | $O(1)$ write, but locks required. | $O(V)$ scan of row. |
| **Semantic Validation** | Complex and slow. | Harder to detect cycles. | Harder to trace paths. | **Optimal** for cycle and connectivity detection. | Complex due to state pollution. | Fast for small graphs, slow for sparse. |
| **Execution Isolation** | Clean. | Clean. | Clean. | **Excellent**. Shared read-only structure. | **Poor**. Threading hazards across runs. | Clean. |
| **Maintainability** | Simple, but slow. | Simple. | Simple. | **High**. Separates indexing logic. | Poor. Duplicates IWS semantics. | Harder to trace named task IDs. |
| **Recovery Interaction** | Easy. | Easy. | Easy. | **Clean**. Re-derived or loaded. | Fragile. Complex state sync. | Easy. |

---

## 10. Decision
NexusFlow will define the workflow dependency topology using an immutable, execution-independent bidirectional directed graph derived from IWS declarations.

1.  **Ingestion & Validation Lifecycle**: Graph construction participates in a two-phase validation lifecycle:
    $$\text{Candidate IWS} \xrightarrow{\text{Derive}} \text{Dependency Projection} \xrightarrow{\text{Semantic Validation (ADR-004)}} \text{Validated IWS} \text{ \& } \text{Canonical Workflow Graph}$$
    *   **Dependency Projection**: A temporary, pre-validation structural representation that may contain cycles, self-dependencies, disconnected nodes, or missing target references. It is not assumed to be a valid DAG.
    *   **Canonical Workflow Graph**: The finalized, immutable directed acyclic graph (DAG) ready for scheduler consumption once validation has passed.
2.  **Bidirectional Sparse Representation**: The graph tracks connections using bidirectional adjacency maps:
    *   `nodes`: `Set<TaskDefinitionId>`
    *   `dependencies`: `TaskDefinitionId -> Set<TaskDefinitionId>` (incoming edges)
    *   `dependents`: `TaskDefinitionId -> Set<TaskDefinitionId>` (outgoing edges)
3.  **Semantic Identity Parity**: Nodes are keyed strictly by their canonical `TaskDefinitionId` from the IWS. The graph must not introduce domain-level UUIDs for nodes. Implementation-level indexing (e.g., array indexes) may only be used internally for optimization and must not replace semantic identity.
4.  **No Semantic Duplication**: Graph nodes must not duplicate non-topological fields from the IWS (e.g. Activity Type, timeout, or retry policies). The IWS remains the sole authority for definition semantics.
5.  **Strict Immutability**: Once validated and accepted, both the IWS and its corresponding `Canonical Workflow Graph` are immutable. Mutating nodes or edges during execution is strictly forbidden.

---

## 11. Decision Rationale
*   **Validator Enablement**: Option A (generating a `Dependency Projection` before validation) is selected because semantic validators (ADR-004) require a graph topology representation to validate cyclic paths or disconnected sub-graphs efficiently. 
*   **Operational Traversal Performance**: Bidirectional adjacency indexes ensure that the scheduler can retrieve prerequisite counts in $O(indegree(T))$ and propagate completions in $O(outdegree(T))$, avoiding expensive $O(V + E)$ scans.
*   **Concurrency & Execution Safety**: Complete separation of static graph topology from active execution states allows concurrent executions to share read-only topology without execution-state synchronization on graph contents.

---

## 12. Tradeoffs
*   **Duplicate Edge Storage**: Storing bidirectional adjacency means every logical edge `A -> B` is represented twice in memory (once in `dependents[A]` and once in `dependencies[B]`). This minor memory footprint increase is accepted in exchange for $O(1)$ average map lookup times.
*   **Stricter Projection Parser**: The graph builder must be robust enough to compile a cyclic or disconnected `Dependency Projection` without encountering stack overflows or recursive parsing crashes.

---

## 13. Consequences
*   **Pure-Function Derivation**: Deriving the `Dependency Projection` from the IWS is a deterministic, stateless operation.
*   **Separate Dynamic State**: The scheduler must maintain execution eligibility counters (e.g., `remaining_dependencies`) inside localized execution models rather than inside the graph representation itself.
*   **Decoupled Extensibility**: In V1, edges represent only structural execution constraints. Conditional execution branching or failure-handling logic is decoupled and deferred to later scheduling/execution ADRs.

---

## 14. Failure Modes

| Failure Mode | Cause | Consequence | Mitigation | Owning ADR |
| :--- | :--- | :--- | :--- | :--- |
| **Cycles in Ingestion** | Circular dependencies in Candidate IWS (e.g., `A -> B -> A`). | Cyclic projection built. | Projection builder maps cyclic paths without loops; validator rejects. | [ADR-003](adr-003-canonical-workflow-graph-representation.md) / [ADR-004](00-architecture-decision-register.md#L195) |
| **Self Dependency** | Task declares itself as a dependency (`A -> A`). | Self-loop in projection. | Represent edge in projection; validator checks and rejects. | [ADR-003](adr-003-canonical-workflow-graph-representation.md) / [ADR-004](00-architecture-decision-register.md#L195) |
| **Missing Node Reference** | Task `B` declares dependency on missing task `X`. | Unresolved edge in projection. | Do not create phantom nodes; expose unresolved targets; validator rejects. | [ADR-003](adr-003-canonical-workflow-graph-representation.md) / [ADR-004](00-architecture-decision-register.md#L195) |
| **Diverged Adjacency Maps** | Implementation bug yields inconsistent forward/reverse maps. | Invalid scheduler execution paths or deadlocks. | Implement bidirectional consistency assertions in testing suites. | [ADR-003](adr-003-canonical-workflow-graph-representation.md) / [ADR-021](00-architecture-decision-register.md#L224) |
| **Duplicate Declarations Ingested** | Multiple identical semantic dependencies (`A -> B` and `A -> B`). | Duplicate edges in projection. | Projection preserves duplicate counts for validator diagnostics before deduplicating. | [ADR-003](adr-003-canonical-workflow-graph-representation.md) / [ADR-004](00-architecture-decision-register.md#L195) |
| **Accidental Mutation** | Scheduler code attempts to write execution states into graph nodes. | Race conditions; multi-execution data corruption. | Expose the accepted graph through immutable/read-only interfaces and enforce immutability through implementation-level safeguards and tests. | [ADR-003](adr-003-canonical-workflow-graph-representation.md) |
| **Version/Derivation Mismatch** | Engine update alters graph parsing rules for historical definition data. | Recovery failures or altered execution order. | Running executions must remain associated with the exact immutable definition semantics from which their topology was derived. The binding/versioning mechanism is deferred to ADR-011/012/024. | [ADR-012](00-architecture-decision-register.md#L211) / [ADR-024](00-architecture-decision-register.md#L231) |

---

## 15. Debugging Considerations
*   **Detached Coordinate Provenance**: Validation errors occurring in the graph must correlate back to file line/column coordinates from [ADR-002](adr-002-workflow-definition-parsing-strategy.md) using a detached mapping, shielding the graph model from text coordinate metadata.
*   **Adjacency Dumps**: The graph abstraction must provide a debugging string format that outputs a readable adjacency representation (e.g. showing dependencies and dependents lists for each task).

---

## 16. Testing Considerations
*   **Bidirectional Map Consistency**: Write unit tests verifying that for every node `A` containing `B` in its dependents, `B` contains `A` in its dependencies.
*   **Topological Invariant Coverage**: Test graph derivation against isolated tasks, linear chains, large fan-outs, disconnected topologies, cycles, and self-loops.

---

## 17. Operational Considerations
*   **Memory Efficiency**: Because the graph is immutable, active executions can read the same cached graph instance in memory, eliminating redundant graph structures.
*   **Sparse Graph Advantage**: Adjacency lists are highly memory-efficient for real-world workflow structures, which are typically sparse.

---

## 18. Maintenance Considerations
*   **Unspecified Defaults**: The graph builder never owns retry, timeout, or execution-policy defaults. Their definitions and resolution belong to the ADRs governing those domain semantics; ADR-002 only resolves authoring-level normalization where appropriate.
*   **No Speculative Features**: We reject adding conditional edge metadata or trigger predicates in V1. Adding conditional edges requires a separate future architectural decision.

---

## 19. Future Evolution
*   **Graph Re-derivation on Recovery**: If recovery (ADR-012) requires rebuilding the definition in memory, the engine can deterministically regenerate the graph from the persisted IWS.
*   **Version Pinning**: Running executions must remain associated with the exact immutable definition semantics from which their topology was derived. The binding/versioning mechanism is deferred to ADR-011/012/024.

---

## 20. Rejected Alternatives
*   **Direct IWS Traversal (Alternative A)**: Rejected due to scheduling performance penalties. Running $O(V)$ scans on every task completion degrades execution throughput.
*   **Unidirectional Adjacency Maps (Alternatives B/C)**: Rejected. Tracking only dependents makes checking prerequisites a $O(V)$ scan; tracking only dependencies makes finding downstream tasks to trigger a $O(V)$ scan.
*   **Rich Mutable Nodes (Alternative E)**: Rejected. Bleeding execution states into graph nodes prevents sharing definition structures across concurrent runs and introduces thread-synchronization overhead.
*   **Adjacency Matrix (Alternative F)**: Rejected due to $O(V^2)$ memory scaling, which wastes excessive space for sparse real-world graphs.

---

## 21. Decision Evolution
*   **V1 Draft (2026-08-30)**: First draft established the pre-validation `Dependency Projection` lifecycle, bidirectional adjacency map formats, and strict boundary rules separating definition topology from runtime scheduler states.

---

## 22. Common Misconceptions
*   *Misconception*: "The graph stores whether a task is running or completed."
    *   *Correction*: The graph is strictly read-only. Runtime execution states are maintained independently by the Scheduler ([ADR-005](00-architecture-decision-register.md#L200)) and Execution engine ([ADR-006](00-architecture-decision-register.md#L201)).
*   *Misconception*: "The graph defines which task runs first."
    *   *Correction*: The graph defines structural topology. The Scheduler determines execution eligibility and prioritization based on runtime conditions.

---

## 23. Open Questions
*   Should the graph compute and cache static topological sorting orders for diagnostic trace logs? *Deferred to validation testing needs.*

---

## 24. Interview Discussion

### Why build a separate graph representation if dependencies are already declared in the IWS?
The IWS is structured to capture semantic concepts (policies, settings, and declarations). Querying dependencies directly from the IWS requires searching list fields, which is inefficient. The graph acts as an optimized, index-like traversal structure designed for $O(1)$ average map lookup times.

### Why store both incoming and outgoing adjacency? Isn't that redundant?
Without outgoing adjacency (`dependents`), propagating task completion to downstream tasks requires searching all task dependency lists, which is $O(V + E)$. Without incoming adjacency (`dependencies`), verifying if a task is eligible requires an $O(V)$ check. Storing both edges as references enables optimal scheduler traversal times.

### What is the exact difference between Dependency Projection and Canonical Workflow Graph?
The Dependency Projection is the initial, pre-validation structural mapping derived from Candidate IWS. It may contain cycles, self-dependencies, or invalid node targets. The Canonical Workflow Graph is the finalized DAG constructed only after semantic validation (ADR-004) successfully verifies the projection.

### Where should execution-local counters like "remaining dependency count" be stored?
Execution-local counters must live in runtime memory structures (such as task scheduling contexts or execution trackers) owned by the Scheduler ([ADR-005](00-architecture-decision-register.md#L200)) and State Machine ([ADR-006](00-architecture-decision-register.md#L201)). Placing them in the graph would break concurrent execution isolation.

### Why not introduce conditional-edge metadata in V1 to prevent future refactoring?
Speculative edge structures (like conditions or triggers) complicate the V1 DAG traversal and cycle-detection mechanics. Conditional branch semantics should be designed when runtime requirements are settled rather than as a guess in V1.

---

## 25. References
*   [ADR-001: Internal Workflow Specification](adr-001-internal-workflow-specification.md)
*   [ADR-002: Workflow Definition Parsing Strategy](adr-002-workflow-definition-parsing-strategy.md)
*   [NexusFlow Architectural Register](00-architecture-decision-register.md)

---

## 26. Traceability
*   **Depends On**: [ADR-001: IWS](adr-001-internal-workflow-specification.md)
*   **Implements**: Capability 4
*   **Related To**: [ADR-002: Parsing](adr-002-workflow-definition-parsing-strategy.md), [ADR-004: Validation](00-architecture-decision-register.md#L195)

---

## 27. Decision Validation Checklist
*   [x] Is the problem statement decoupled from specific database/broker technologies?
*   [x] Are the functional requirements (FRs) and non-functional requirements (NFRs) traced?
*   [x] Were at least two realistic candidate designs critically evaluated?
*   [x] Are the tradeoffs clear (what are we giving up for simplicity or correctness)?
*   [x] Does the design preserve all mapped system invariants?
*   [x] Does this decision avoid introducing tight coupling between modules?
*   [x] Are the potential failure modes mapped?
*   [N/A] Is there a clear explanation of how this design behaves during a graceful shutdown? *(Justification: Decoupled domain representation; graceful shutdown logic is owned by execution components under ADR-017).*
*   [x] Are the debugging strategies defined?
*   [N/A] Does the testing strategy explain how to simulate failures and recovery? *(Justification: Topological path testing is unit-level; recovery simulation belongs to execution recovery under ADR-012).*
*   [x] Are the performance limits and resource footprints qualitatively identified?
*   [N/A] Is the future evolution path to HA or multi-tenant deployment explained? *(Justification: Decoupled design; high-availability and clustering concerns are deferred to ADR-025).*
*   [x] Can this decision be defended during an SDE-2 engineering review?
