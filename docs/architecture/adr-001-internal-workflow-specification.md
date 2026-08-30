# ADR-001 — Internal Workflow Specification (IWS)

*   **Status**: Under Review
*   **Last Updated**: 2026-08-30
*   **Deciders**: Project Owner
*   **Domain**: Foundation (Definition & Representation)
*   **Criticality**: Critical
*   **Relationships**:
    *   **Implements**: Capability 1
    *   **Related To**: [ADR-002: Workflow Definition Parsing Strategy](00-architecture-decision-register.md#L193), [ADR-003: Canonical Workflow Graph Representation](00-architecture-decision-register.md#L194)

---

## 1. Purpose
The purpose of this document is to define the architectural role, characteristics, boundaries, and lifecycle of the **Internal Workflow Specification (IWS)**. The IWS serves as the canonical, format-independent semantic domain representation of a workflow definition within the NexusFlow orchestration engine.

---

## 2. Context
NexusFlow initial releases accept workflow definitions authored in YAML. However, the system's core orchestration capabilities must not be coupled to YAML, JSON, or any specific external presentation. In the future, the platform must support workflow definitions generated via Python and other language SDKs, dedicated domain-specific languages (DSLs), visual designers, and dynamic API endpoints.

Without a clean abstraction, external formats, parsing concerns, and validation rules leak directly into the scheduler, state machine, and persistence systems. This leads to tight coupling, makes system evolution fragile, and increases the difficulty of adding alternative client interfaces.

---

## 3. Problem Statement
How can NexusFlow support multiple, diverse external workflow-authoring representations (YAML, JSON, SDKs, DSLs) without coupling the core scheduling and execution engine to the schema, parser, or quirks of any specific external authoring representation?

---

## 4. Requirements Covered
*   **Capability 1**: Format-agnostic workflow definition ingestion.
*   **NFR-POR-003 (Portability)**: Ability to support future language SDKs and alternative authoring frontends without modifying the scheduler.
*   **Core Principle**: Technology Independence Before Implementation.
*   **Core Principle**: Every Abstraction Must Solve a Real Problem.

---

## 5. Constraints
*   **Version 1 Scope**: Single-node execution engine with no active distributed-consensus or dynamic clustering logic.
*   **Technology Neutrality**: The specification must not be defined in terms of specific programming language structures (e.g., specific Java/Python classes) or storage formats (e.g., PostgreSQL JSONB).
*   **No Runtime Modification**: The workflow execution model must not alter the core definition semantics during scheduling or task execution.

---

## 6. Goals
*   Establish a format-independent, canonical semantic representation (IWS) for workflow definitions.
*   Enforce a clean conceptual boundary between raw external definitions, normalized candidate representations, and validated immutable specifications.
*   Decouple the workflow's semantic definition from the runtime traversal graph and active execution states.
*   Ensure that the internal model is fully serializable to support persistent storage, transmission, and offline debugging.

---

## 7. Non-Goals
*   Choosing the concrete parser libraries or defining parsing strategies (deferred to [ADR-002](00-architecture-decision-register.md#L193)).
*   Defining the concrete traversal data structures, graph node formats, or scheduler traversal algorithms (deferred to [ADR-003](00-architecture-decision-register.md#L194)).
*   Defining specific semantic validation rules or parsing error syntax (deferred to [ADR-004](00-architecture-decision-register.md#L195)).
*   Designing the workflow versioning model, migration strategies, or schema evolution mechanisms (deferred to [ADR-024](00-architecture-decision-register.md#L231)).
*   Selecting database technologies, persistence schemas, or communication protocols (deferred to [ADR-011](00-architecture-decision-register.md#L210) & [ADR-020](00-architecture-decision-register.md#L223)).

---

## 8. Candidate Solutions

### Alternative A — Directly Consume External YAML/JSON in the Engine
Under this approach, the core orchestrator and scheduler operate directly on parsed map-like structures (e.g., `Map<String, Object>`) parsed from the source YAML/JSON.
*   **Mechanism**: The parser converts raw files into in-memory maps or generic document objects. The engine reads fields directly from these maps during scheduling and traversal.

### Alternative B — Generic Normalized Serialization-Centric Document (JSON)
Convert all incoming formats into a standardized JSON document. The scheduler and other engine components parse and read fields from this normalized JSON schema.
*   **Mechanism**: A YAML parser translates the source file into a generic JSON string. The execution engine operates on this JSON string or standard JSON objects (e.g., `JsonObject`) using a JSON Schema to enforce API contracts.

### Alternative C — Abstract Syntax Tree (AST) as Internal Representation
Parse external representations into a compiler-like Abstract Syntax Tree (AST), which preserves syntax-level details, expressions, and grammar rules.
*   **Mechanism**: Parsers build an AST containing node positions, comments, expression blocks, and syntax structures. The engine evaluates this AST dynamically during execution.

### Alternative D — Dedicated Canonical Semantic IWS (Domain Model)
Create a language-independent, serialization-independent semantic domain model.
*   **Mechanism**: Raw inputs are normalized by parser adapters into a **Candidate IWS**. The candidate is semantically validated. Upon passing, a **Validated IWS** is constructed. This object is structurally immutable and decoupled from AST nodes, serialization layouts, and execution state.

---

## 9. Detailed Evaluation of Every Candidate

| Criteria | Alternative A (Direct YAML) | Alternative B (Normalized JSON) | Alternative C (AST) | Alternative D (Canonical IWS) [Selected] |
| :--- | :--- | :--- | :--- | :--- |
| **Coupling Implications** | Severe. Core engine is directly bound to source file syntax/structure. | Moderate. Engine is bound to the target serialization format (JSON). | High. Engine is bound to syntactic grammar constructs. | **Minimal**. Engine depends on abstract domain semantics. |
| **Validation Implications** | Hard. Rules are scattered; validation must happen at runtime during read. | Moderate. Schema validation helps, but domain invariants are hard to enforce on raw JSON. | Complex. Requires compiling or traversing language-specific syntax trees. | **Clean**. Separate validation stage transitions Candidate to Validated. |
| **Recovery Implications** | Fragile. Changing parser versions or file formats mid-flight risks crashing active executions. | Moderate. JSON parser stability is high, but schema drift causes runtime failures. | Hard. Rebuilding state requires re-executing syntax nodes. | **Excellent**. Immutable specifications isolate execution boundaries. |
| **Extensibility Implications** | Poor. Adding a Python SDK requires converting python code back to mock YAML. | Fair. SDKs can produce JSON, but schema additions pollute generic objects. | High. Suitable for DSL expressions, but overhead is high for simple tasks. | **Outstanding**. Adapter layer separates authoring formats from core logic. |
| **Operational & Debugging** | Hard to trace changes or verify model correctness in memory. | Good serializability, but lacks clear domain-type safety in engine logs. | Extremely complex to debug scheduler state traversal. | **Optimal**. Explicit domain contracts trace directly to execution. |

---

## 10. Decision
NexusFlow will introduce the **Internal Workflow Specification (IWS)** as the canonical semantic representation of workflow definitions.

1.  **Normalization Pipeline**: External configurations must traverse a defined lifecycle:
    `External Definition` $\rightarrow$ `Parsing/Normalization` $\rightarrow$ `Candidate IWS` $\rightarrow$ `Semantic Validation` $\rightarrow$ `Validated IWS`.
2.  **Strict Immutability**: A `Validated IWS` is immutable. Any modification to a workflow definition results in a new instance of `Validated IWS`.
3.  **Separation from Traversal**: The IWS is separate from the `Canonical Workflow Graph` ([ADR-003](00-architecture-decision-register.md#L194)).
4.  **Format-Agnostic Serializable Contract**: The model must be fully serializable to support persistence, reconstruction, debugging, and future transmission needs, without tying the domain schema to any single serialization library or technology stack.
5.  **Execution Association**: An execution must be unambiguously associated with a specific immutable `Validated IWS` instance.

---

## 11. Decision Rationale
*   **Separation of Concerns**: Decoupling the source format from the core scheduling engine ensures that the engine only deals with domain semantics, achieving *Technology Independence*.
*   **Deterministic Safety**: Enforcing immutability post-validation eliminates a major class of runtime bugs where workflow definitions are altered mid-execution.
*   **Extensibility Shield**: Designing this layer in V1 ensures that when Python SDKs or specialized DSLs are added in the future, the core orchestration engine does not need to be refactored.

---

## 12. Tradeoffs
*   **Translation Overhead**: Every workflow definition must be converted from its source representation into the internal representation. This adds conversion latency and minor memory allocation overhead during ingestion.
*   **Duplication of Schema**: The system must maintain mapping layers between external schemas (like the YAML schema) and internal specifications. Changes to the workflow schema require updates to both the parser/adapter logic and the internal spec definitions.

---

## 13. Consequences
*   **No Direct Engine Access**: Downstream components (Scheduler, State Machine) are blocked from accessing raw external inputs or parsing diagnostics.
*   **Strict Mapping Layer Requirement**: Every new client representation (e.g., Python SDK) must implement a parser/compiler adapter that targets the IWS.
*   **Clear Implementation Roadmap**: Establishes a clean separation between [ADR-002: Parsing](00-architecture-decision-register.md#L193) (producing Candidates) and [ADR-004: Validation](00-architecture-decision-register.md#L195) (producing Validated outputs).

---

## 14. Failure Modes

| Failure Mode | Cause | Consequence | Mitigation | Owning ADR |
| :--- | :--- | :--- | :--- | :--- |
| **Malformed Ingestion** | Syntactically incorrect YAML/JSON provided. | Normalization failure; exception thrown. | Fast-fail at API boundary; return syntax error to user. | [ADR-002](00-architecture-decision-register.md#L193) |
| **Diverged Semantics** | Different parsers yield different semantic trees for similar constructs. | Execution behavior differs depending on authoring medium. | Shared compliance suite validating normalization consistency. | [ADR-002](00-architecture-decision-register.md#L193) / [ADR-021](00-architecture-decision-register.md#L224) |
| **Invalid Candidates Ingested** | Schema matches, but contains semantic errors (e.g., circular tasks). | Engine crashes or deadlocks during graph construction. | Strictly isolate Candidate from Validated IWS using validation guards. | [ADR-004](00-architecture-decision-register.md#L195) |
| **Accidental Spec Mutation** | Code attempts to modify validated definition fields at runtime. | Side-effects leak into concurrent or future executions. | Enforce structural immutability through language constraints (e.g., readonly/immutable). | [ADR-001](00-architecture-decision-register.md#L192) |
| **Stale Derived Graphs** | Graph builder uses outdated or mutated definition fields. | Inconsistent task ordering; invalid scheduler states. | Derivation is a pure function: `BuildGraph(ValidatedIWS) -> Graph`. | [ADR-003](00-architecture-decision-register.md#L194) |
| **Version Drift** | IWS schema evolves; historical executions cannot locate matching specs. | Recovery fails; active executions crash on node restart. | Executions bind to historical definition state; migrations handle schema changes. | [ADR-012](00-architecture-decision-register.md#L211) / [ADR-024](00-architecture-decision-register.md#L231) |

---

## 15. Debugging Considerations
*   **Original Source Retention**: While the engine runs on IWS, the original authoring source (YAML text) and schema lines may/should be retainable alongside the IWS to support localized diagnostics (e.g., mapping a runtime task failure to the exact line number of the user's YAML file), with the concrete retention and storage policies deferred to future persistence and API decisions.
*   **Domain Representation Dumps**: The IWS must expose a debugging output format (e.g., structured print/string format) that dumps the semantic elements of the workflow definition to the execution logs.

---

## 16. Testing Considerations
*   **Equivalence Testing**: Implement tests where identical logic authored via different formats (e.g., YAML vs. mock programmatic builder) results in equivalent Candidate and Validated IWS properties.
*   **Mutation Protection Tests**: Write tests that attempt to cast and mutate IWS records/structs, asserting compilation failures or runtime safety violations.

---

## 17. Operational Considerations
*   **Memory Footprint**: Because definitions are immutable, multiple active executions can share a single cached reference to a `Validated IWS` instance in memory.
*   **Definition Sharing & Caching**: Because `Validated IWS` representations are immutable, multiple concurrent executions may safely share references to the same definition representation in memory. The concrete details of caching, eviction, and garbage collection policies are deferred to implementation.

---

## 18. Maintenance Considerations
*   **Semantic Fields Containment**: As new scheduler features are added (e.g., custom retry profiles), the definition-time semantics of those configurations belong in the IWS specification. Runtime scheduler policy and node-level configurations do not.
*   **Adapter Maintenance**: New client interfaces are isolated to parser extensions, shielding the scheduler codebase from dependency upgrades of external parsers (e.g., YAML parser package updates).

---

## 19. Future Evolution
*   **Distributed Storage**: When moving to distributed scheduling (V2+), the IWS serialization guarantees allow the specification to be compiled once and distributed across worker clusters.
*   **Definition Hashing & Fingerprinting**: A future persistence ADR may introduce deterministic content-addressable hashes of the IWS to identify versions uniquely and optimize database key mappings.

---

## 20. Rejected Alternatives

*   **Direct YAML Consumption (Alternative A)**: Rejected due to coupling. Leaking file-parsing structures directly into the scheduler makes the core scheduling system fragile and blocks the ability to support Python or other programmatic SDKs.
*   **Normalized Serialization JSON (Alternative B)**: Rejected as a primary model. Relying on a JSON schema/document as the core engine model forces scheduling logic to directly depend on the serialization structure rather than a semantic domain contract. While JSON remains a strong candidate for persistent representation or external storage, the serialization format should not define the engine's internal semantic boundary.
*   **Syntax AST Consumption (Alternative C)**: Rejected because syntax details (whitespaces, formatting tokens, parsing rules) are irrelevant to orchestration execution. An AST introduces overhead and leaks syntax structures to traversal components.

---

## 21. Decision Evolution
*   **V1 Draft (2026-08-30)**: First draft established the normalization pipeline, Candidate vs. Validated lifecycle, decoupling of semantic models from graph structures, and the boundaries between execution states and definition properties.

---

## 22. Common Misconceptions
*   *Misconception*: "IWS is a JSON/YAML file format."
    *   *Correction*: IWS is an in-memory semantic domain model. JSON or YAML are external serialization formats.
*   *Misconception*: "IWS executes the workflow."
    *   *Correction*: IWS is a static description. The Scheduler ([ADR-005](00-architecture-decision-register.md#L200)) and State Machine ([ADR-006](00-architecture-decision-register.md#L201)) evaluate the derived graph.

---

## 23. Open Questions
*   Should the IWS preserve human-readable metadata like author, department, or validation timestamps, or should these live strictly in external management databases? *Deferred to API and Persistence layers.*
*   Does a task retry logic declaration belong to Task Definition or Workflow Definition level? *Deferred to ADR-007.*

---

## 24. Interview Discussion

### Why not use YAML directly?
Directly consuming YAML couples the engine to YAML's syntax rules, parsing libraries, and structural limits. It makes supporting Python/Go SDKs or custom DSLs extremely difficult, violating Technology Independence.

### What is the difference between IWS and an AST?
An Abstract Syntax Tree (AST) represents the syntactic structure of an input file (expressions, language-specific tokens, grammar rules). The IWS represents domain semantics—it describes tasks, dependencies, and execution configurations, regardless of how they were written.

### What is the difference between IWS and the dependency graph?
The IWS is the static semantic model (containing metadata, data bindings, configurations, and raw dependency declarations). The dependency graph is a derived, optimized data structure built from the IWS specifically for traversal, loop detection, and topological execution ordering.

### Why is a Validated IWS immutable?
If a definition changes while executions are in progress, mutable definition objects can cause race conditions, state corruption, or invalid execution progressions. Immutability ensures that an execution's behavior is deterministic from start to finish.

### Why shouldn't runtime state live inside the IWS?
Conflating static definitions with dynamic execution state prevents sharing definition models across concurrent executions, breaks caching, and violates separation of concerns.

---

## 25. References
*   [NexusFlow Architectural Register](00-architecture-decision-register.md)
*   Domain Driven Design (Evans, 2003) — Canonical Models.

---

## 26. Traceability
*   **Implements**: Capability 1
*   **Precursor To**: [ADR-002: Parsing](00-architecture-decision-register.md#L193), [ADR-003: Graph](00-architecture-decision-register.md#L194)
*   **Related To**: [ADR-004: Validation](00-architecture-decision-register.md#L195), [ADR-010: Data Flow](00-architecture-decision-register.md#L205)

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
*   [N/A] Does the testing strategy explain how to simulate failures and recovery? *(Justification: Failure recovery mechanics are deferred to ADR-012; testing here focuses on schema equivalence and mutation protection).*
*   [x] Are the performance limits and resource footprints qualitatively identified?
*   [x] Is the future evolution path to HA or multi-tenant deployment explained?
*   [x] Can this decision be defended during an SDE-2 engineering review?
