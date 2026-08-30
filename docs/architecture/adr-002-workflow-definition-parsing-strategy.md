# ADR-002 — Workflow Definition Parsing Strategy

*   **Status**: Under Review
*   **Last Updated**: 2026-08-30
*   **Deciders**: Project Owner
*   **Domain**: Foundation (Definition & Representation)
*   **Criticality**: Core
*   **Relationships**:
    *   **Depends On**: [ADR-001: Internal Workflow Specification](adr-001-internal-workflow-specification.md)
    *   **Implements**: FR-WDV-001, FR-WDV-004

---

## 1. Purpose
This document defines the ingestion architecture for workflow definitions entering the NexusFlow orchestration engine. It establishes the separation of concerns between syntax-level parsing, structural validation, normalization, and semantic domain validation, specifying the lifecycle through which raw client definitions converge on a canonical `Candidate IWS`.

---

## 2. Context
NexusFlow must accept human-authored workflow definitions and translate them into the engine's core internal representation (the `Candidate IWS` defined in [ADR-001](adr-001-internal-workflow-specification.md)). 

For Version 1, the primary input representation is YAML. However, the system's architecture must scale to support programmatic compilation pathways (such as Python/Go SDK builders), visual designer graphs, and custom compiled DSLs. Without a strict boundary separating parser-specific and syntax-centric conventions from engine domain concepts, syntax changes or new authoring frontends will contaminate the engine's core components.

---

## 3. Problem Statement
How should NexusFlow validate and transform raw external workflow representations into a canonical `Candidate IWS` while ensuring strict ingestion correctness, extensible support for multiple frontends (YAML, SDKs, DSLs), and robust error diagnostics?

---

## 4. Requirements Covered
*   **FR-WDV-001**: YAML workflow definition parsing.
*   **FR-WDV-004**: Ingestion validation and error reporting.
*   **NFR-POR-003**: Extensibility to alternative client definition formats.
*   **Core Principle**: Technology Independence Before Implementation.
*   **Core Principle**: Explicit Behaviour Over Implicit Behaviour.

---

## 5. Constraints
*   **Format Separation**: The scheduler and state machine must have no awareness of YAML-specific schemas, file syntax, or parsing library types.
*   **Stateless Processing**: Definition translation must be stateless and free of side-effects relative to other active or registered workflows.
*   **Diagnostic Integrity**: Ingestion errors must retain clear pointers to original source coordinates (e.g. line and column) without embedding these coordinate metadata fields in the canonical semantic models (`Candidate IWS` or `Validated IWS`).

---

## 6. Goals
*   Provide a robust, decoupled parsing and normalization pipeline for YAML definitions in V1.
*   Establish the boundary where format-specific syntax translates into format-independent semantics.
*   Define a strict, fail-fast validation policy for all ingested structures.
*   Ensure that different authoring frontends (YAML, Python SDKs, DSL compilers) converge on equivalent Candidate IWS semantics for equivalent logical workflows.

---

## 7. Non-Goals
*   Selecting the concrete YAML parsing library or dependencies (deferred to implementation).
*   Defining the public API error JSON representation or HTTP response structure (deferred to [ADR-015](00-architecture-decision-register.md#L220)).
*   Establishing semantic domain-level checks such as task cycle detection or cross-task dependency existence checks (deferred to [ADR-004](00-architecture-decision-register.md#L195)).
*   Defining the execution graph traversal models or adjacency list formats (deferred to [ADR-003](00-architecture-decision-register.md#L194)).

---

## 8. Candidate Solutions

### Alternative A — Direct External Format to Candidate IWS Mapping
Parsers directly map file fields into the canonical `Candidate IWS` model in a single pass.
*   **Mechanism**: A parser reads raw YAML and deserializes it directly into the target `Candidate IWS` domain objects.

### Alternative B — Frontend-Specific Source Definition Model with Normalization Layer [Selected]
Each authoring frontend translates its input into an intermediate representation suited to its syntax (for YAML, the **Source Definition Model**), validates its structure, and normalizes it to a common `Candidate IWS`.
*   **Mechanism**: The YAML frontend parses raw text into a syntax-centric `Source Definition Model` (preserving shorthands, optional omissions, and aliases). It performs structural checks and then normalized the structure into the clean, format-independent `Candidate IWS`. Future SDKs/DSLs compile their logic into their own intermediate models before normalization.

### Alternative C — Generic Canonical Serialization Document (JSON Document)
All authoring formats are transformed into a standardized, generic JSON document. The scheduler and engine query this JSON document directly using JSON Schema constraints.
*   **Mechanism**: Inputs are parsed to standard JSON, and the engine accesses fields via JSON pathing or a standard map structure.

---

## 9. Detailed Evaluation of Every Candidate

| Criteria | Alternative A (Direct Parse) | Alternative B (Source Model & Normalization) [Selected] | Alternative C (Generic JSON) |
| :--- | :--- | :--- | :--- |
| **Mechanism** | Parse text directly to target domain object. | Parse to Source Model $\rightarrow$ Validate Structure $\rightarrow$ Normalize to Candidate IWS. | Parse to a generic raw JSON tree. |
| **Complexity** | Low for V1. Fewer classes and mapping steps. | Moderate. Requires mapping code and separate models. | High runtime complexity. Hard to write strongly typed logic against a map. |
| **Coupling** | High. Domain model must expand to support YAML-specific syntactic sugar. | **Minimal**. Source Model shields Domain Model from frontend syntax changes. | High. Logic is bound to serial JSON representation details. |
| **Diagnostics** | Poor. Direct mapper errors are often low-level parsing exceptions. | **Excellent**. Source Model validates shape and maps errors to source metadata. | Hard to map validation issues to file-line levels. |
| **Future Frontends** | Poor. A Python SDK would have to mock file-deserialization mappings. | **Optimal**. Every frontend compiles its custom structures to the same Candidate IWS contract. | Fair. SDK can output JSON, but lacks compile-time validation benefits. |
| **Security** | Harder to isolate tag-bombs or unsafe tags before instantiation. | **Good**. Parser limits are enforced during Source Model construction. | Good, but parsing generic maps lacks schema-type constraints. |
| **Maintainability** | Poor. Changing a YAML shorthand breaks scheduler property mappings. | **High**. Syntax conventions are isolated to the translation layer. | Low. Lack of schema compiler support leads to runtime fragility. |

---

## 10. Decision
NexusFlow will define the ingestion boundary around an **Authoring Frontend** contract that produces a canonical `Candidate IWS`.

1.  **Ingestion Pipeline**: The translation process follows a strict sequence:
    `External Ingest` $\rightarrow$ `Parsing/Construction` $\rightarrow$ `Source Model` $\rightarrow$ `Structural Validation` $\rightarrow$ `Normalization` $\rightarrow$ `Candidate IWS`.
2.  **No Single Source Model Rule**: We reject the creation of a single universal "Source Model" for all future frontends. Each frontend (YAML, SDK, DSL) utilizes the intermediate representation (e.g., AST, builder model, or YAML Source Definition Model) appropriate for its input structure.
3.  **Strict Ingestion Policy**: Ingestion must fail fast and reject ambiguous inputs (such as unknown fields, duplicate YAML keys, incorrect types, and unsupported YAML tags/features) rather than guessing intent.
4.  **Decoupled Provenance**: Ingestion frontends must retain source coordinates (file, line, column, original path) sufficient to support downstream diagnostics. This metadata must be stored in a detached manner (e.g. sidecar mapping) rather than polluting the IWS schema.
5.  **Failure Atomicity**: Ingestion is atomic. A failure at any stage of parsing, structural validation, or normalization must immediately halt the pipeline, returning an error and exposing no partially constructed Candidate IWS to downstream systems.

---

## 11. Decision Rationale
*   **Technology Neutrality**: Restricting format details to the authoring frontend preserves the technology independence of the core engine.
*   **Shorthand Decoupling**: The engine operates on canonical, explicit semantics. Normalization expands syntactic conveniences (e.g. converting shorthand strings into structured properties) before the Candidate IWS is created, simplifying downstream traversal.
*   **Strictness over Permissiveness**: In distributed workflow engines, silent configuration interpretation is dangerous. Enforcing strict, fail-fast parsing prevents tasks from executing with unintended defaults due to typos (such as `retrys: 3`).

---

## 12. Tradeoffs
*   **Boilerplate Mapping**: The translation layer requires maintaining mapping code between Source Models and Candidate IWS. This increases V1 codebase size and test maintenance.
*   **Performance Overhead**: Introducing an intermediate representation allocates short-lived objects during definition ingestion. This is acceptable because definition registration occurs far less frequently than scheduler execution.

---

## 13. Consequences
*   **Clear ADR Boundaries**: Establishes a clean boundary for [ADR-004 (Validation)](00-architecture-decision-register.md#L195): ADR-002 answers "Can we parse and normalize this?", whereas ADR-004 answers "Is this normalized configuration semantically valid?".
*   **Downstream Protection**: Downstream components (Scheduler, State Machine) can assume that any provided IWS represents a structurally sound definition with all shorthands fully normalized.

---

## 14. Failure Modes

| Failure Mode | Cause | Consequence | Mitigation | Owning ADR |
| :--- | :--- | :--- | :--- | :--- |
| **Malformed YAML** | Invalid syntax (e.g. bad indentation). | Parsing exception. | Catch and throw structured Syntax Error with source coordinates. | [ADR-002](adr-002-workflow-definition-parsing-strategy.md) |
| **Duplicate Keys** | Same key declared twice in a map. | Ambiguous configuration. | Configure YAML parser to fail on duplicate keys; do not silently overwrite. | [ADR-002](adr-002-workflow-definition-parsing-strategy.md) |
| **Unknown Fields** | Typo in configuration (e.g. `retrys`). | Unintended execution defaults. | Enforce strict schema verification during structural validation. | [ADR-002](adr-002-workflow-definition-parsing-strategy.md) |
| **Normalization Failure** | Malformed shorthand or invalid format strings. | Normalization error; ingestion fails. | Validate input structures before mapping; throw structured conversion error. | [ADR-002](adr-002-workflow-definition-parsing-strategy.md) |
| **YAML Parser Tags Exploited** | Malicious YAML payload containing recursive anchors or constructor tags. | Denial of Service (DoS) or arbitrary code execution. | Disable custom YAML tag instantiation; restrict max nesting depths and file sizes. | [ADR-002](adr-002-workflow-definition-parsing-strategy.md) / [ADR-022](00-architecture-decision-register.md#L225) |
| **Drift Across Frontends** | Python SDK and YAML parser normalize similar semantics differently. | Diverged execution behavior. | Implement shared conformance tests verifying normalization equivalence. | [ADR-002](adr-002-workflow-definition-parsing-strategy.md) / [ADR-021](00-architecture-decision-register.md#L224) |
| **IWS Schema Evolution** | Historical workflow definitions become incompatible with the current IWS schema. | Recovery fails or active executions crash. | Defer resolution to persistence, recovery, and versioning strategies. | [ADR-011](00-architecture-decision-register.md#L210) / [ADR-012](00-architecture-decision-register.md#L211) / [ADR-024](00-architecture-decision-register.md#L231) |

---

## 15. Debugging Considerations
*   **Coordination of Source Diagnostics**: Frontends must support a mechanism to correlate validation errors back to source lines (e.g., via a detached coordinate map that links a Candidate IWS node ID to its source file span).
*   **Ingestion Trace Logs**: Ingestion steps (parse success, normalization start, mapping results) must produce explicit debug traces.

---

## 16. Testing Considerations
*   **Strictness Validation**: Test suites must verify that all categories of malformed inputs (invalid types, duplicate keys, unknown fields) trigger appropriate ingestion errors.
*   **Conformance Equivalence**: Write cross-frontend tests asserting that programmatic model builds and parsed YAML sources generate equivalent Candidate IWS properties.

---

## 17. Operational Considerations
*   **Resource Limits**: Resource limits must be enforceable at the ingestion boundary; exact values are implementation/security-policy decisions.
*   **Stateless Processing**: Ingestion should not depend on mutable state created by previous ingestion requests, allowing independent processing of definitions.

---

## 18. Maintenance Considerations
*   **Definition-Time Semantic Changes**: Updates to definition schema requirements (e.g., adding task timeouts) will require updating the IWS model, structural validation filters, and frontend mapping schemas.
*   **Parser Upgrades**: Changes to parsing libraries must be validated against a regression test suite to ensure default parsing behaviors (such as duplicate keys or float representation) do not drift.

---

## 19. Future Evolution
*   **Alternative Frontend Plugins**: The decoupled compiler architecture allows new formats (like visual JSON graphs) to be added simply by implementing a new frontend translation adapter converging on Candidate IWS.
*   **Version-Aware Ingestion**: In the future (ADR-024), version headers on incoming definitions will route configurations to different schema-specific normalization mappings.

---

## 20. Rejected Alternatives
*   **Direct Ingestion to Domain (Alternative A)**: Rejected to prevent syntax conveniences from polluting the domain model and to keep the core scheduler decoupled from external formats.
*   **Generic JSON Document (Alternative C)**: Rejected because a generic serialization document should not become the engine's semantic domain contract. Relying on raw JSON forces core engine logic to couple directly to a serialization structure rather than an explicit domain contract.

---

## 21. Decision Evolution
*   **V1 Draft (2026-08-30)**: Defined the separation of parsing, structural validation, and normalization. Established strict parsing, duplicate key rejection, and the detached source metadata requirement.

---

## 22. Common Misconceptions
*   *Misconception*: "The Source Definition Model is the public API contract."
    *   *Correction*: The Source Definition Model is an internal intermediate syntax model used by the frontend compiler, not the final API transport DTO.
*   *Misconception*: "Normalization checks if task dependencies are correct."
    *   *Correction*: Normalization only converts structural syntax. The verification of dependencies and cycle checks is semantic validation, owned by [ADR-004](00-architecture-decision-register.md#L195).

---

## 23. Open Questions
*   How should custom extension metadata (e.g., UI visual layout properties) be handled by normalization? *Deferred to future API / visual dashboard ADRs.*

---

## 24. Interview Discussion

### Why introduce a Source Definition Model? Isn't this overengineering?
If the YAML representation maps directly into the domain model, the domain model must be modified every time we support a new shorthand or authoring convenience. The Source Definition Model isolates syntax changes from core orchestration logic, preserving Technology Independence.

### What is the difference between parsing and normalization?
Parsing reads raw bytes and builds the intermediate syntax model (Source Model). Normalization translates this syntax model into canonical domain semantics (Candidate IWS) by expanding shorthands and resolving syntax-level defaults.

### Why reject unknown fields? Doesn't that hurt forward-compatibility?
For execution engines, silent typos are highly dangerous (e.g. typing `retry_cont` instead of `retry_count` would result in no retries). Strict parsing ensures user configurations behave exactly as written. Forward compatibility is managed via explicit version indicators or dedicated extension namespaces rather than silent permissiveness.

### How do downstream validation errors map back to YAML line numbers?
Frontends generate a detached source coordinate map that links parsed object identifiers to their original source coordinates (e.g., line and column). When semantic validation (ADR-004) fails, the validator queries this sidecar map to output human-friendly line numbers.

### Why not require all frontends to implement the same parser interface?
A YAML input file must be parsed as text, while a Python SDK constructs the definition programmatically. Forcing both into an identical "text parser" interface is an incorrect abstraction. The only mandatory contract is that all frontends output a valid `Candidate IWS` object.

---

## 25. References
*   [ADR-001: Internal Workflow Specification](adr-001-internal-workflow-specification.md)
*   [NexusFlow Architectural Register](00-architecture-decision-register.md)

---

## 26. Traceability
*   **Depends On**: [ADR-001: IWS](adr-001-internal-workflow-specification.md)
*   **Precursor To**: [ADR-004: Validation](00-architecture-decision-register.md#L195)
*   **Implements**: FR-WDV-001, FR-WDV-004

---

## 27. Decision Validation Checklist
*   [x] Is the problem statement decoupled from specific database/broker technologies?
*   [x] Are the functional requirements (FRs) and non-functional requirements (NFRs) traced?
*   [x] Were at least two realistic candidate designs critically evaluated?
*   [x] Are the tradeoffs clear (what are we giving up for simplicity or correctness)?
*   [x] Does the design preserve all mapped system invariants?
*   [x] Does this decision avoid introducing tight coupling between modules?
*   [x] Are the potential failure modes mapped?
*   [N/A] Is there a clear explanation of how this design behaves during a graceful shutdown? *(Justification: Decoupled translation layer; graceful shutdown concerns are owned by execution runners under ADR-017).*
*   [x] Are the debugging strategies defined?
*   [N/A] Does the testing strategy explain how to simulate failures and recovery? *(Justification: Parser error testing is standard unit testing; recovery simulation belongs to execution layers under ADR-012).*
*   [x] Are the performance limits and resource footprints qualitatively identified?
*   [N/A] Is the future evolution path to HA or multi-tenant deployment explained? *(Justification: Decoupled design; high-availability and clustering concerns are deferred to ADR-025).*
*   [x] Can this decision be defended during an SDE-2 engineering review?
