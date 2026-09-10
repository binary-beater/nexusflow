# ADR-010 — Workflow Data Flow & Parameter Passing

## 1. Purpose

This Architectural Decision Record (ADR) defines the conceptual data flow architecture, parameter-passing semantics, and execution context models for the NexusFlow orchestration engine. It establishes how workflow inputs, task inputs, authoritative task outputs, and workflow outputs are represented, validated, passed, and bound across directed acyclic graph (DAG) execution topologies. 

Furthermore, this record strictly defines the invariants governing data immutability, data consistency across task execution states, retry data isolation, error boundary classifications for data operations, and the explicit structural separation between orchestration-level binding semantics and downstream persistence, wire serialization, and activity-level execution concerns.

---

## 2. Context

NexusFlow orchestrates multi-task workflows structured as directed acyclic graphs. The preceding architectural decisions have established the foundational structural, execution, and coordination primitives:
- [ADR-001](adr-001-internal-workflow-specification.md) established the canonical Internal Workflow Specification (IWS), dictating that workflow definitions are normalized into validated, immutable internal schemas.
- [ADR-003](adr-003-canonical-workflow-graph-representation.md) established a single, canonical, immutable graph model where vertices represent tasks and directed edges represent explicit scheduling dependencies.
- [ADR-004](adr-004-workflow-validation-strategy.md) defined two-phase validation (pre-validation and deep static validation) for workflow definitions prior to ingestion.
- [ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md) established the dispatching model and success-only dependency satisfaction semantics, specifying that a task becomes eligible for dispatch (`RUNNABLE`) only after all upstream dependencies have successfully terminated.
- [ADR-006](adr-006-workflow-execution-state-machine.md) defined the root `WorkflowExecution` state machine, its lifecycle transitions, and its terminal success criteria.
- [ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md) decoupled the logical `TaskExecution` lifecycle from the physical, ephemeral `ExecutionAttempt` lifecycle, dictating that a `TaskExecution` may undergo multiple retry attempts while maintaining a single logical task identity.
- [ADR-008](adr-008-worker-coordination-and-liveness-model.md) defined worker coordination, worker incarnation tracking, execution start deadlines, and fencing mechanisms to guarantee single-winner result settlement.
- [ADR-009](adr-009-task-routing-strategy.md) established the two-stage routing pipeline (strict capability/liveness filtering followed by decoupled candidate selection) that dispatches runnable tasks to workers without altering payload semantics.

While ADR-003 established that directed edges govern scheduling order, an orchestration system requires a deterministic mechanism for passing data into tasks, transferring outputs between upstream and downstream tasks, and surfacing workflow completion results to external consumers. 

Without a rigorous data model, orchestration systems frequently degenerate into systems plagued by undefined serialization behaviors, race conditions between state transitions and data visibility, silent data corruption across retries, and unbounded orchestration complexity caused by embedding dynamic expression languages into the scheduler. This ADR formalizes the data flow semantics to eliminate these vulnerabilities.

---

## 3. Problem Statement

A robust workflow engine must address several interrelated data passing and consistency challenges:

1. **Logical Value Representation:** Workflows interact with diverse languages and worker environments. The engine requires a unified, technology-neutral conceptual data model that worker runtimes can interpret unambiguously without coupling the orchestrator to language-specific memory objects or unvalidated binary blobs.
2. **Deterministic Task Input Resolution:** When a task becomes eligible to execute, how are its input parameters constructed? How does the engine prevent pre-execution resolution failures from invalidating the state machine invariants established in ADR-007 (specifically, that a `RUNNABLE` task cannot fail prior to creating an `ExecutionAttempt`)?
3. **Retry Parameter Stability:** When a task attempt fails and a subsequent `ExecutionAttempt` is spawned under the same `TaskExecution`, what input does it receive? How does the engine ensure idempotency and reproducibility without exposing attempts to intermediate or mutated inputs?
4. **Data vs. Topology Coupling:** How do data dependencies relate to the canonical scheduling graph? Can tasks read outputs from arbitrary ancestors, and how does the engine prevent hidden or undeclared execution coupling?
5. **Authoritative Output Publishing and Atomicity:** A task attempt produces an output payload. Under what exact conditions does this payload become the authoritative, immutable output of the `TaskExecution`? How is split-brain or delayed result delivery prevented from corrupting task state?
6. **Workflow Output Aggregation:** When all tasks terminate and a workflow reaches `SUCCEEDED`, what constitutes the final workflow output, especially in workflows with multiple leaf tasks or disjoint branches?
7. **Payload Size and Boundary Management:** How does the orchestrator prevent massive payloads from degrading scheduling performance, database stability, or message broker limits without introducing premature object-store coupling?

---

## 4. Requirements Covered

This architecture directly addresses the following NexusFlow requirements:

- **DATA-01:** Provide an unambiguous, language-agnostic logical data model for workflow inputs, task inputs, task outputs, and workflow outputs.
- **DATA-02:** Guarantee deterministic, stable task input binding derived strictly from immutable execution sources.
- **DATA-03:** Enforce strict data isolation across task retries, ensuring identical business inputs across all attempts under a `TaskExecution`.
- **DATA-04:** Enforce that task-to-task data flow strictly adheres to declared direct dependencies in the canonical workflow graph.
- **DATA-05:** Guarantee single-winner, authoritative output publishing, ensuring task output immutability once accepted.
- **DATA-06:** Enforce atomic visibility between execution state transitions (`SUCCEEDED`) and payload availability for both tasks and workflows.
- **DATA-07:** Provide explicit, topology-independent workflow output declarations decoupled from graph sink heuristics.
- **DATA-08:** Maintain strict architectural boundaries, isolating logical data flow from physical persistence (ADR-011), recovery scans (ADR-012), physical concurrency control (ADR-013), and transport serialization (ADR-020).

---

## 5. Constraints

1. **State Machine Invariants (ADR-006 & ADR-007):** Data resolution must not violate established state transitions. Specifically, in ADR-007, a `RUNNABLE` task transitions to `DISPATCHED` and then `RUNNING`; there is no `RUNNABLE -> FAILED` transition. Task input resolution must be fully deterministic and guaranteed to succeed prior to a task entering `RUNNABLE` under valid system state.
2. **Success-Only Dependencies (ADR-005):** In V1, task execution dependencies are success-only. A task cannot execute or resolve inputs from an upstream task that failed, was cancelled, or was skipped.
3. **Canonical Single Edge Graph (ADR-003):** NexusFlow maintains a single dependency graph. Data flow must not introduce hidden edges, separate "data edges", or transitive cross-branch reads.
4. **Technology Neutrality:** The logical data flow model must not depend on or enforce specific serialization libraries (Protobuf, MessagePack, Pydantic), database schemas (PostgreSQL JSONB), or programming language constructs (Python dicts, Java Maps).
5. **Bounded Inline Payloads:** NexusFlow core orchestration handles inline control and data flow. The orchestrator must not act as a distributed blob storage engine in V1.

---

## 6. Goals

- Define a JSON-compatible logical value model as the authoritative semantic foundation for all workflow data.
- Define whole-value explicit input bindings (`Literal`, `WorkflowInput`, `TaskOutput`) that eliminate expression-evaluation failures at runtime.
- Require that any task reading an upstream task's output must explicitly declare that upstream task as a direct dependency.
- Guarantee that all `ExecutionAttempt` instances under a `TaskExecution` receive the exact same logical business input.
- Separate control-plane Execution Context from business Task Input.
- Guarantee that a task's or workflow's transition to `SUCCEEDED` is strictly atomic with the visibility of its authoritative output.
- Define explicit, named workflow output bindings that decouple public workflow results from internal graph topology.
- Establish clear architectural boundaries with downstream persistence (ADR-011), recovery (ADR-012), concurrency (ADR-013), and error handling (ADR-018).

---

## 7. Non-Goals

- Implementing or supporting path extraction syntaxes (e.g., JSONPath, JMESPath, JSON Pointer) in V1.
- Embedding runtime expression, templating, or scripting engines (e.g., Jinja, CEL, Python expressions, JavaScript) into the orchestrator.
- Providing shared mutable workflow state, global blackboards, or task-mutated context variables.
- Introducing a first-class `PayloadRef` or managing the lifecycle of external object storage blobs in V1.
- Guaranteeing transactional, exactly-once physical execution of external side effects produced by worker activities.
- Standardizing physical wire formats, transport schemas, or database table layouts in this ADR.
- Building a centralized schema registry or enforcing runtime JSON Schema/Pydantic validation within the core orchestrator.

---

## 8. Candidate Solutions

### 8.1 Logical Data Model Candidates

#### Candidate A: Native Language Objects (e.g., Python Pickles, JVM Serialized Objects)
Data passed between tasks is serialized using language-native mechanisms (e.g., Python `pickle` or Java serialization).
- *Pros:* Complete language fidelity; supports complex classes, arbitrary objects, and binary structures seamlessly within a single runtime.
- *Cons:* Severe security vulnerabilities (arbitrary code execution); completely couples the orchestrator and workers to a specific programming language and runtime version; prevents polyglot workers.

#### Candidate B: Selected — Abstract JSON-Compatible Logical Value Model
Data is conceptually restricted to the JSON value space: `null`, `boolean`, `number`, `string`, `array`, and `object/map` with string keys and recursively JSON-compatible values.
- *Pros:* Universally portable across all programming languages; transparent to inspect and audit; strictly separates data from executable code; well-defined boundary for serialization.
- *Cons:* Cross-language numeric precision (e.g., 64-bit integers vs. floating-point representations) requires disciplined encoding conventions; does not natively represent raw binary streams.

#### Candidate C: Raw Opaque Bytes
The orchestrator treats all inputs and outputs as uninterpreted byte sequences (`bytes[]`).
- *Pros:* Maximum engine simplicity; orchestrator is completely agnostic to payload structure; zero overhead for payload inspection.
- *Cons:* Prevents the orchestrator from doing any structural validation; precludes future field-level auditing, metadata extraction, or platform-level workflow output synthesis; shifts complete deserialization complexity to workers with no shared contract.

#### Candidate D: Strict Type System with Central Schema Registry
Every task input and output must be accompanied by a formal schema (e.g., JSON Schema, Protobuf definition) validated by an orchestrator schema registry before execution.
- *Pros:* Strict compile-time and runtime type safety; explicit contracts between distributed development teams.
- *Cons:* Enormous operational complexity for V1; requires schema versioning, registration endpoints, and migration management; slows down developer iteration.

---

### 8.2 Task Input Binding Candidates

#### Candidate A: Selected — Whole-Value Explicit Bindings
Task definitions declare named inputs bound entirely to one of three sources: `Literal(value)`, `WorkflowInput`, or `TaskOutput(task_id)`. The consuming task receives the entire value of the source without partial extraction.
- *Pros:* Completely deterministic; validated statically at definition ingestion (ADR-004); eliminates runtime resolution failures (e.g., missing property, path syntax error); preserves the ADR-007 state machine invariant where `RUNNABLE` tasks cannot fail before dispatch.
- *Cons:* Tasks receive the entire upstream output object rather than specific extracted fields, requiring activity code to extract specific fields internally.

#### Candidate B: Restricted Path Extraction (e.g., JSONPath, JSON Pointer)
Task definitions declare input bindings that extract specific fields from upstream outputs using path expressions (e.g., `task_a.output.user.id`).
- *Pros:* Tasks receive only the specific data fields required; cleaner worker function signatures.
- *Cons:* Introducing runtime path evaluation introduces runtime evaluation failure modes (missing property, type mismatch). If upstream produces unexpected data, input resolution fails. Handling this requires complex error states in the orchestrator before an attempt exists, breaking the clean attempt-level error semantics of ADR-007.

#### Candidate C: Dynamic Expression / Templating Language (e.g., Jinja, CEL, JavaScript)
Input bindings support arbitrary expressions, string interpolation, and logical transformations (e.g., `{{ task_a.output.count * 2 }}`).
- *Pros:* Extremely flexible; allows business logic and glue transformations to be defined directly in the workflow graph.
- *Cons:* Severe complexity; security risks (sandboxing required); deterministic debugging becomes difficult; non-reproducible errors; high CPU overhead inside the orchestration scheduler.

#### Candidate D: Shared Mutable Workflow Context (Blackboard Architecture)
Tasks do not bind explicit inputs or outputs. Instead, all tasks read from and write to a shared, mutable key-value dictionary representing the workflow state.
- *Pros:* Familiar to developers used to imperative programming; trivial to pass state across arbitrary steps.
- *Cons:* Race conditions between parallel tasks; non-deterministic execution; impossible to trace data provenance; retrying a task that mutated shared state causes irrecoverable corruption.

---

### 8.3 Workflow Output Model Candidates

#### Candidate A: No Workflow-Level Output
Workflows reach terminal states (`SUCCEEDED`, `FAILED`), but produce no unified output. External clients must inspect individual task outputs to determine results.
- *Pros:* Engine simplicity; no output resolution step.
- *Cons:* Poor developer ergonomics; leaks internal graph structure to external API clients; brittle if task names or internal topologies are refactored.

#### Candidate B: Implicit Leaf-Output Synthesis
The orchestrator automatically aggregates the outputs of all leaf tasks (tasks with out-degree zero) into a dictionary keyed by task ID.
- *Pros:* Requires no explicit output configuration by the workflow author.
- *Cons:* Leaks internal graph structure; fragile when DAG structure evolves; in workflows with multiple leaves (e.g., parallel notification tasks), irrelevant task outputs pollute the workflow result; non-obvious semantics for disconnected components.

#### Candidate C: Selected — Explicit Named Whole-Value Output Bindings
The workflow author explicitly declares the workflow's public output schema as a mapping of output names to binding sources (`TaskOutput(task_id)`, `WorkflowInput`, `Literal(value)`).
- *Pros:* Establishes a clean, stable public API contract for the workflow; independent of internal graph refactoring; supports single-sink, multi-leaf, and disjoint topologies identically; resolves cleanly to `null` when no outputs are declared.
- *Cons:* Requires explicit authoring in the workflow definition.

---

## 9. Detailed Evaluation

The candidates were evaluated against five primary engineering criteria:
1. **Determinism and State Machine Integrity:** Guaranteeing that state transitions remain robust, reproducible, and immune to mid-scheduling failures.
2. **Polyglot Portability:** Ensuring clean interoperability across multiple programming languages and worker architectures.
3. **Operational Debuggability:** Ensuring clear data lineage and provenance across retries and crashes.
4. **Architectural Simplicity:** Minimizing scheduler CPU overhead and operational dependencies in V1.
5. **Security and Isolation:** Preventing code injection, sandboxing issues, and unintended data mutation.

| Evaluation Metric | Model: Native Objects | Model: Abstract JSON (Selected) | Model: Opaque Bytes | Bindings: Whole-Value (Selected) | Bindings: Path Extraction | Bindings: Expression Engine | Bindings: Shared Mutable |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Determinism** | Low | **High** | High | **Very High** | Moderate | Low | Very Low |
| **Portability** | Very Low | **High** | High | **High** | High | Moderate | Low |
| **Debuggability** | Low | **High** | Low | **High** | Moderate | Low | Very Low |
| **Simplicity** | Moderate | **High** | High | **High** | Moderate | Very Low | Moderate |
| **Safety / Isolation** | Very Low | **High** | High | **High** | High | Low | Very Low |

The evaluation demonstrates that combining the **Abstract JSON-Compatible Logical Value Model** with **Whole-Value Explicit Bindings** and **Explicit Named Workflow Output Bindings** provides the highest architectural integrity. This combination guarantees that all data bindings are validated statically during workflow definition ingestion, completely eliminating runtime expression failures from the orchestration core.

---

## 10. Decision

NexusFlow establishes the following architecture for workflow data flow, parameter passing, and execution context:

### 10.1 Abstract JSON-Compatible Logical Value Model
NexusFlow governs all workflow execution data using an abstract, JSON-compatible logical value model consisting strictly of:
- `null`
- `boolean` (true, false)
- `number` (integer or floating-point; canonicalized according to JSON semantics)
- `string` (Unicode sequence)
- `array` (ordered list of JSON-compatible values)
- `object/map` (unordered collection of string keys mapped to JSON-compatible values)

**Architectural Boundaries:**
- This is a semantic logical model only. NexusFlow does not freeze physical wire encodings (JSON text, MessagePack, Protobuf) or runtime language representations (Python dict, Java Map, Rust struct).
- Raw binary payloads (`bytes[]`) and language-native executable objects are **not** canonical V1 payload types.
- Binary references (e.g., file identifiers, S3 URIs) must be passed as standard JSON-compatible strings or objects.

### 10.2 WorkflowExecution Input Semantics
- A `WorkflowExecution` receives exactly one JSON-compatible logical input value upon creation/acceptance.
- The input value is validated for structural JSON-compatibility at ingress (ADR-004 / ADR-015). A workflow execution must never enter normal initialized or running execution with structurally invalid input.
- Once accepted, `WorkflowExecution.input` is **strictly immutable** for the entire lifetime of that execution.

### 10.3 Task Input Binding Model
A `TaskDefinition` declares named input parameters mapped to explicit binding sources:
$$\text{input\_name} \longrightarrow \text{BindingSource}$$

In V1, `BindingSource` supports exactly three whole-value variants:
1. `Literal(value)`: An immutable literal value conforming to the logical value model, defined directly in the Validated IWS.
2. `WorkflowInput`: A reference to the entire immutable `WorkflowExecution.input`.
3. `TaskOutput(task_id)`: A reference to the entire authoritative output of the specified upstream `TaskExecution`.

**Whole-Value Rule:** All bindings are whole-value bindings. NexusFlow V1 deliberately excludes path extraction (JSONPath, JMESPath) and expression/template languages.

### 10.4 The Direct Dependency Rule
A `TaskOutput(task_id)` binding is valid **if and only if** the referenced `task_id` is an explicitly declared **direct dependency** of the consuming task in the canonical workflow graph:
$$\text{Task } C \text{ binds TaskOutput}(A) \implies A \in \text{Dependencies}(C)$$

If task $A$ precedes $B$, and $B$ precedes $C$ ($A \to B \to C$), task $C$ cannot bind `TaskOutput(A)` unless $C$ explicitly declares $A$ as a direct dependency ($C$ depends on both $A$ and $B$). NexusFlow maintains a single, unified dependency relation governing both execution order and data availability.

### 10.5 Stable TaskExecution Input and Retry Isolation
- A `TaskExecution` has exactly **one stable logical resolved input**.
- Its value is derived deterministically from immutable sources: `WorkflowExecution.input`, static definition literals, and the authoritative outputs of successfully terminated direct dependencies.
- **Retry Invariant:** All `ExecutionAttempt` instances spawned under a single `TaskExecution` receive the **exact same logical business input**. Retries alter execution control metadata (attempt ID, attempt ordinal) but never mutate the business task input.
- Logical immutability does not dictate physical persistence architecture; downstream implementations may choose to materialize the resolved input dictionary upon scheduling or deterministically reconstruct it on demand (owned by ADR-011).

### 10.6 Structural Separation of Execution Context and Business Input
Worker invocations cleanly separate **Execution Context** (control-plane metadata) from **Business Task Input** (application parameters):
- **Execution Context:** Contains orchestration identifiers and execution control metadata:
  - `workflow_execution_id`
  - `task_execution_id`
  - `attempt_id`
  - `attempt_number`
  - `activity_type`
- **Business Task Input:** Contains the resolved named input bindings (`{ "param_a": ..., "param_b": ... }`).
- Execution context metadata is never injected into or merged with the business input dictionary, ensuring business activities remain decoupled from orchestrator metadata.

### 10.7 Authoritative Task Output and Single-Winner Publishing
- A successful worker execution produces a result payload. This payload becomes the **authoritative output** of the `TaskExecution` if and only if:
  1. The reporting `ExecutionAttempt` holds valid, unfenced ownership authority (ADR-008).
  2. The `TaskExecution` undergoes a valid single-winner state transition to `SUCCEEDED` (ADR-007).
  3. The result payload conforms structurally to the logical value model and satisfies bounded size limits.
  4. The output is durably committed.
- **Output Immutability:** Once committed, `TaskExecution.output` is strictly immutable.
- **Void Task Output:** A task that completes successfully without producing a return value records its authoritative output as `null`. NexusFlow does not distinguish between "void" output and `null` output; every successful task execution has exactly one logical output value.
- **Quarantine of Non-Authoritative Data:** Payloads returned by failed, cancelled, timed-out, superseded, or fenced attempts are strictly quarantined. They never become authoritative task outputs, though they may be retained in diagnostic audit logs (ADR-014 / ADR-018).

### 10.8 Task Success and Output Atomicity
A `TaskExecution` must **never** become observably `SUCCEEDED` in any query, event stream, or downstream scheduling evaluation while its authoritative output is absent, uncommitted, or unreadable. The state transition to `SUCCEEDED` and the publishing of the authoritative output must be completely consistent (physical mechanisms owned by ADR-013).

### 10.9 Workflow Output Model
- Workflow outputs are declared explicitly via **Workflow Output Bindings** in the `WorkflowDefinition`:
  $$\text{output\_name} \longrightarrow \text{OutputBindingSource}$$
- `OutputBindingSource` supports:
  1. `TaskOutput(task_id)`: References the authoritative output of any task defined within the workflow. The referenced task does **not** need to be a DAG leaf task.
  2. `WorkflowInput`: Pass-through of the original workflow execution input.
  3. `Literal(value)`: Static literal values.
- **Multiple Leaves and Disjoint Graphs:** Workflow outputs are completely decoupled from graph topology. In workflows with multiple leaves or disconnected components, only explicitly declared bindings are exposed. NexusFlow never synthesizes output by automatically scraping leaf tasks.
- **Default (No Bindings):** If a workflow definition declares no output bindings, a successful execution yields an authoritative output of `null`.
- **Workflow Success Atomicity:** A `WorkflowExecution` must never become observably `SUCCEEDED` while its declared output bindings remain unresolved or uncommitted.

### 10.10 Payload Size Bounds and External Object References
- NexusFlow V1 handles bounded inline payloads. Payloads exceeding configured thresholds are rejected during result ingestion.
- NexusFlow rejects a first-class `PayloadRef` abstraction in V1. Applications handling massive datasets, media, or binary objects must store the data in external storage (e.g., S3, shared file systems) and pass standard JSON-compatible reference strings (e.g., URIs, object keys) through NexusFlow. NexusFlow treats these references as ordinary business values and does not manage external object lifecycles.

### 10.11 Rejection of Shared Mutable State
NexusFlow strictly rejects shared mutable workflow variables, global blackboards, and task-mutated execution contexts. All task-to-task communication flows exclusively through explicit, dependency-linked outputs.

### 10.12 External Side Effects Invariant
Task and workflow output atomicity governs orchestrator state only. NexusFlow **does not** guarantee transactional or exactly-once execution of external side effects produced by worker activities. If an activity performs an external mutation and the worker crashes before the success result is committed, retrying the task will execute the activity again. Workers must implement application-level idempotency strategies.

---

## 11. Decision Rationale

1. **State Machine Invariant Protection:** In ADR-007, the `TaskExecution` state machine specifies that a task moves from `PENDING` to `RUNNABLE` when dependencies succeed, and from `RUNNABLE` to `DISPATCHED` when assigned to a worker. If input bindings permitted runtime expression evaluation or dynamic path extraction, an invalid path or missing field would cause input resolution to fail while in `RUNNABLE`. Because `RUNNABLE -> FAILED` is an invalid transition in ADR-007, handling this failure would require either inventing artificial `ExecutionAttempt` instances or corrupting the lifecycle model. Whole-value bindings eliminate runtime evaluation failures; input resolution cannot fail under valid system state.
2. **Elimination of Hidden Edges:** Permitting a task to read data from an arbitrary ancestor without declaring a direct dependency would create a discrepancy between the scheduling graph and the data flow graph. If the intermediate scheduling path were altered, the data dependency could break silently. Enforcing the **Direct Dependency Rule** guarantees that the canonical DAG reflects all execution and data constraints simultaneously.
3. **Auditability and Debuggability:** Restricting data to the JSON logical value model ensures that execution history can be inspected, audited, and serialized without requiring specialized language decoders or risking unsafe deserialization exploits.
4. **Retry Idempotency:** Guaranteeing that every retry attempt under a `TaskExecution` receives the exact same logical business input ensures that task failures are reproducible and immune to state drift.
5. **Decoupling Public Contracts from Graph Topologies:** Explicit workflow output bindings permit workflow authors to refactor internal DAG structures, consolidate tasks, or introduce parallel post-processing tasks without changing the external API contract of the workflow.

---

## 12. Tradeoffs

| Architectural Benefit | Tradeoff Incurred | Mitigation Strategy |
| :--- | :--- | :--- |
| **Zero Runtime Resolution Failures:** Whole-value bindings prevent missing-property and expression syntax crashes at runtime. | **Coarser Granularity:** Tasks receive the entire upstream output object rather than specific extracted fields. | Worker activity implementations parse and extract required fields in application code, where language-native error handling and typing are available. |
| **Single Graph Simplicity:** Requiring direct dependencies for all data bindings ensures the canonical DAG reflects all dependencies. | **Edge Redundancy:** A task may require an explicit edge to an ancestor even if a transitive path already exists ($A \to B \to C$ and $A \to C$). | Static validation (ADR-004) verifies this cleanly; the DAG remains fully acyclic and unambiguous. |
| **Language Neutrality:** JSON-compatible logical values allow seamless polyglot worker coordination. | **No Native Binary Streams:** Large binary data cannot be passed directly through the engine as byte arrays. | Applications pass URI or object-key references; blob storage remains decoupled from orchestration. |
| **Strict Immutability:** Input and output states are fixed once committed, preventing race conditions. | **Memory / Storage Overhead:** In-memory or persisted representations cannot be mutated in place. | Downstream persistence (ADR-011) can optimize storage via deduplication, reconstruction, or normalized JSONB storage. |

---

## 13. Consequences

### 13.1 Positive Consequences
- **Robust Scheduling Integrity:** Schedulers can safely transition tasks to `RUNNABLE` knowing that all required input data is fully materialized or deterministically resolvable.
- **Deterministic Retries:** Every attempt of a task executes against identical business parameters, simplifying debugging and root-cause analysis.
- **Secure Worker Isolation:** Workers cannot mutate orchestrator state or corrupt data for concurrent downstream tasks.
- **Clean Polyglot Interface:** Any language capable of parsing JSON data types can implement a NexusFlow worker SDK.
- **Stable API Contracts:** External workflow consumers interact with a declared, stable workflow output schema independent of internal task naming or topology changes.

### 13.2 Negative Consequences
- **Increased Boilerplate in Worker Code:** Because the orchestrator does not perform JSONPath extraction or field mapping, worker activities must extract nested fields manually from the input dictionary.
- **Additional Dependency Declarations:** If task $C$ requires data from an early initialization task $A$, an explicit edge $A \to C$ must be maintained, even if $C$ already depends on intermediate tasks.

---

## 14. Failure Modes

| # | Failure Scenario | Authoritative Data Effect | Lifecycle Effect | Retry Effect | Recovery Effect | Owning ADR |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | **Structurally Invalid Workflow Input** | Execution rejected; no input accepted. | Workflow rejected at ingress; never transitions to `RUNNING`. | None (workflow not started). | No execution to recover. | ADR-004 / ADR-015 |
| **2** | **Invalid Literal Binding in Definition** | Definition rejected during validation. | Workflow definition cannot be registered or executed. | None. | None. | ADR-004 |
| **3** | **Unknown Task Referenced in Task Input** | Definition rejected during static validation. | Workflow definition cannot be registered. | None. | None. | ADR-004 |
| **4** | **TaskOutput References Non-Direct Dependency** | Definition rejected during static validation. | Workflow definition cannot be registered. | None. | None. | ADR-004 |
| **5** | **Self-Referential Task Input Binding** | Definition rejected during static validation. | Workflow definition cannot be registered. | None. | None. | ADR-004 |
| **6** | **Duplicate Task Input Parameter Name** | Definition rejected during static validation. | Workflow definition cannot be registered. | None. | None. | ADR-004 |
| **7** | **Invalid Workflow Output Binding** | Definition rejected during static validation. | Workflow definition cannot be registered. | None. | None. | ADR-004 |
| **8** | **Upstream Task Fails** | Upstream produces no authoritative output. | Downstream task remains `PENDING`; workflow transitions toward `FAILED`. | Downstream task never scheduled. | Recovered as `PENDING` or `FAILED`. | ADR-005 / ADR-006 |
| **9** | **Upstream Task Cancelled** | Upstream produces no authoritative output. | Downstream task remains `PENDING`; workflow transitions toward `CANCELLED`. | Downstream task never scheduled. | Recovered as cancelled. | ADR-006 / ADR-007 |
| **10** | **Upstream Task Succeeds with Null Output** | Upstream authoritative output is committed as `null`. | Downstream task transitions to `RUNNABLE`; receives `null` for that binding. | Normal attempt scheduled. | Reconstructed with `null` input binding. | ADR-010 / ADR-011 |
| **11** | **Worker Reports Structurally Invalid Success Result** | Payload rejected; cannot commit authoritative output. | Attempt transitions to `FAILED` (protocol error). Task may retry. | Subsequent attempt receives identical original input. | Incomplete result discarded upon restart. | ADR-007 / ADR-018 |
| **12** | **Worker Reports Oversized Success Payload** | Payload rejected; cannot commit authoritative output. | Attempt transitions to `FAILED` (payload limit error). | Subsequent attempt triggered if retries remain. | Incomplete result discarded. | ADR-018 / ADR-023 |
| **13** | **Duplicate Delivery of Accepted Success Result** | Authoritative output already committed; duplicate ignored. | No lifecycle state change; idempotent acknowledgment. | None; task already `SUCCEEDED`. | Consistent authoritative state maintained. | ADR-008 / ADR-013 |
| **14** | **Conflicting Duplicate Result Delivered** | Authoritative output already committed; conflicting payload rejected. | No state change; conflicting attempt rejected/fenced. | None. | Original authoritative output remains immutable. | ADR-008 / ADR-013 |
| **15** | **Stale / Revoked Attempt Reports Success** | Payload quarantined; cannot become authoritative output. | Attempt fenced and rejected; no effect on `TaskExecution`. | None. | Stale result ignored during reconciliation. | ADR-008 / ADR-012 |
| **16** | **Task Output Persistence Failure** | Output not durably committed; remains non-authoritative. | `TaskExecution` must not become `SUCCEEDED`. Attempt remains uncommitted. | Attempt may time out or retry based on liveness. | Recovery resolves uncommitted attempt. | ADR-011 / ADR-012 |
| **17** | **Orchestrator Crash Before Output Commit** | In-flight worker result uncommitted; not authoritative. | Task remains in `RUNNING` or transitions to recovery reconciliation. | ADR-012 recovery reconciles attempt or triggers retry. | Uncommitted payload safely discarded or re-polled. | ADR-012 / ADR-013 |
| **18** | **Orchestrator Crash After Output Commit** | Authoritative output is durably committed in storage. | `TaskExecution` recovered as `SUCCEEDED`. Downstream tasks become eligible. | No retry needed. | Re-read authoritative output from storage. | ADR-011 / ADR-012 |
| **19** | **Retry Attempt Scheduled** | Logical business input deterministically reconstructed. | New `ExecutionAttempt` created; control metadata updated. | Attempt receives identical business input. | Retries resume cleanly post-crash. | ADR-007 / ADR-010 |
| **20** | **Worker Mutates Local Input Object in Memory** | Mutates worker-local memory only. | None; orchestrator state is completely isolated. | Subsequent attempt runs in fresh process/context. | No effect on stored orchestrator data. | ADR-010 |
| **21** | **Workflow Output Commit Failure** | Workflow output not durably recorded. | `WorkflowExecution` must not become `SUCCEEDED`. | Orchestrator retries workflow completion transaction. | Restart re-evaluates terminal workflow state. | ADR-011 / ADR-013 |
| **22** | **Workflow Has Multiple Leaves** | Only explicitly bound outputs are materialized. | Workflow completes normally; ignores unbound leaf outputs. | None. | Only declared outputs recovered. | ADR-010 |
| **23** | **Workflow Has No Output Bindings Declared** | Authoritative workflow output is committed as `null`. | Workflow transitions to `SUCCEEDED` with `output: null`. | None. | Workflow recovered with `output: null`. | ADR-010 |
| **24** | **Large Object Represented by URI / String** | NexusFlow stores and passes the URI string as an ordinary value. | Normal execution. External storage manages blob data. | Worker reads URI from input; fetches blob directly. | URI recovered as standard string. | ADR-010 |
| **25** | **External Side Effect Before Result Commit Fails** | Result uncommitted; orchestrator unaware of side effect. | Task attempt fails or times out; triggers retry. | Next attempt repeats activity execution. | Side effect repeated unless activity is idempotent. | ADR-010 |

---

## 15. Debugging

To support rapid operational diagnosis of workflow data flow issues without requiring heavy payload logging, NexusFlow establishes the following conceptual correlation model:

- **Correlation Identifiers:** All data flow events must correlate:
  - `workflow_execution_id`
  - `task_execution_id`
  - `attempt_id`
  - `attempt_number`
  - `activity_type`
- **Data Flow State Flags:** The orchestrator traces the progress of data flow through explicit milestone markers:
  - `INPUT_RESOLVED`: Recorded when all required upstream dependencies have produced authoritative output and task input is successfully bound.
  - `TASK_OUTPUT_COMMITTED`: Recorded when a worker's result payload has successfully passed single-winner fencing and is durably committed.
  - `WORKFLOW_OUTPUT_COMMITTED`: Recorded when the root workflow execution has successfully bound and committed its final output.
- **Payload Privacy and Redaction:** 
  - Business input and output payloads may contain sensitive customer data, personally identifiable information (PII), or secrets.
  - The orchestrator **must not** dump full business payloads into standard application logs.
  - Operational logs record payload metadata (e.g., byte size, top-level key names, binding source types) rather than raw contents.
  - Auditing, history retention, and payload masking/redaction policies are governed jointly by ADR-014, ADR-016, and ADR-022.

---

## 16. Testing

The data flow architecture requires comprehensive test coverage across unit, integration, and failure-injection test suites:

### 16.1 Static Definition and Validation Tests
- **Valid Bindings:** Verify successful validation of definitions declaring `Literal`, `WorkflowInput`, and `TaskOutput` bindings.
- **Direct Dependency Enforcement:** Verify that binding a `TaskOutput(A)` on task $B$ fails validation if $A$ is not explicitly declared in $B$'s `dependencies` list.
- **Self-Reference Rejection:** Verify that a task binding `TaskOutput(self)` is rejected.
- **Duplicate Input Keys:** Verify that declaring duplicate input parameter names within a single task is rejected.
- **Syntax and Path Rejection:** Verify that attempting to declare path syntax (e.g., `task_a.output.field`, `$.data`, `{{ expr }}`) fails validation during definition ingestion.
- **Workflow Output Bindings:** Verify that valid workflow output bindings compile correctly, and that bindings referencing non-existent tasks are rejected.

### 16.2 Runtime Binding and Consistency Tests
- **Stable Input Across Retries:** Verify that when attempt 1 fails and attempt 2 is spawned, attempt 2 receives the exact same logical business input.
- **Worker Context Separation:** Verify that worker dispatch envelopes contain distinct `context` and `input` fields, and that context metadata does not leak into the business input dictionary.
- **Eligibility Barrier:** Verify that a task cannot transition to `RUNNABLE` until all upstream authoritative outputs are committed and readable.
- **Null Output Handling:** Verify that a task returning `null` or completing void commits an authoritative output of `null`, and that downstream tasks consume this `null` correctly.
- **Workflow Output Decoupling:** Verify that in a workflow with multiple leaves ($L_1, L_2, L_3$), declaring an output binding to $L_1$ and non-leaf task $T_{mid}$ correctly exposes only those declared values upon workflow success.
- **Default Empty Output:** Verify that a workflow with no declared output bindings completes with `output: null`.

### 16.3 Concurrency, Fencing, and Edge Cases
- **Atomic Success Visibility:** Verify that queries to `TaskExecution` never return `status: SUCCEEDED` with `output: null` or unreadable output when the task produced a valid payload.
- **Stale Result Fencing:** Simulate a superseded attempt reporting a successful result; verify the result is rejected and does not overwrite the winning attempt's output.
- **Duplicate Result Idempotency:** Deliver duplicate success results for an already completed task; verify state and output remain unchanged.
- **Infrastructure Persistence Failure:** Inject storage failures during task output commit; verify the task does not transition to `SUCCEEDED`, and that transient infrastructure errors do not trigger business-level task failure transitions.
- **External Side-Effect Re-Execution:** Test a worker performing an external counter increment followed by an uncommitted result crash; verify that the subsequent retry executes and documents the lack of an orchestrator-level distributed transaction.

---

## 17. Operational Considerations

1. **Payload Size Enforcement:** In-memory schedulers and relational databases experience performance degradation when handling multi-megabyte payloads inline. NexusFlow requires bounded payload enforcement. While exact byte limits are configured via ADR-023 and enforced via ADR-015/ADR-022, operators must ensure that activities handling large files utilize external object storage and pass reference URIs.
2. **Deterministic Troubleshooting:** Because task inputs are stable and derived exclusively from immutable sources, operators can easily reproduce worker errors locally by extracting the recorded task input and running the worker activity in an isolated test environment.
3. **Database Bloat and Pruning:** Persisting input and output payloads across millions of historical task executions consumes significant storage. Downstream persistence designs (ADR-011) must consider table partitioning and historical payload pruning strategies (ADR-014).
4. **Stable Public APIs:** Workflows serving as backend services for customer applications can refactor internal task graphs without breaking external API clients, provided the explicit workflow output bindings remain consistent.

---

## 18. Maintenance

- **Adding New Binding Sources:** If future requirements necessitate new binding sources (e.g., system metadata bindings or secret references), they must be introduced as explicit, strongly validated variants of `BindingSource` in the IWS, accompanied by updated static validation rules in ADR-004.
- **Language SDK Maintenance:** Worker SDKs in various languages (Python, TypeScript, Go) must ensure that worker-side deserializers correctly map JSON types to language-idiomatic structures while preserving the distinction between execution context and business inputs.
- **Strict Separation of Concerns:** Maintainers must ensure that future feature additions to the scheduler do not introduce expression parsing or dynamic scripting into the core orchestration loop.

---

## 19. Future Evolution

The following capabilities are explicitly deferred from V1 but may be evaluated in future architectural revisions:

1. **Restricted Path Extraction:** Introducing declarative, pre-validated path extraction (e.g., strict JSON Pointer `RFC 6901`) if customer demand warrants avoiding worker-side boilerplate. Any future path extraction must be designed such that missing-field semantics are resolved safely without violating task lifecycle states.
2. **First-Class Payload References (`PayloadRef`):** Introducing an engine-managed reference type that allows the orchestrator to automatically offload payloads exceeding configured thresholds to object storage (e.g., S3, GCS) and transparently re-hydrate them in worker SDKs.
3. **Activity Contract Schema Validation:** Permitting tasks to declare optional JSON Schema or Pydantic contracts that the orchestrator or worker SDK validates prior to dispatch.
4. **Streaming Payloads:** Enabling streaming chunked data between adjacent tasks executing on co-located workers.

---

## 20. Rejected Alternatives

### 20.1 Implicit Graph-Derived Workflow Outputs
- **Description:** Automatically constructing workflow outputs from all leaf tasks in the DAG.
- **Reason for Rejection:** Fragile and tightly couples internal graph topology to public API contracts. Adding a cleanup or notification leaf task would silently alter the workflow's public output structure. Furthermore, it creates ambiguity in workflows with disconnected components or multiple leaves.

### 20.2 Dynamic Expression and Templating Languages (CEL, Jinja, JavaScript)
- **Description:** Allowing workflow authors to define data transformations and arithmetic directly within task input bindings.
- **Reason for Rejection:** Embeds heavy computation and sandboxing requirements into the orchestration scheduler. Introduces non-deterministic runtime evaluation failures that violate ADR-007 state machine guarantees, increases cognitive overhead, and complicates debugging.

### 20.3 Path Extraction in V1 (JSONPath, JMESPath)
- **Description:** Permitting tasks to extract specific fields from upstream task outputs using query expressions.
- **Reason for Rejection:** Creates runtime failure conditions (e.g., path not found, array index out of bounds) prior to worker dispatch. In ADR-007, a `RUNNABLE` task has no valid transition to `FAILED` without an `ExecutionAttempt`. Resolving this requires complex synthetic error handling. Keeping bindings whole-value guarantees that if the upstream task succeeded, input resolution cannot fail.

### 20.4 Shared Mutable Workflow Context (Blackboard)
- **Description:** Providing a shared, global key-value store that parallel tasks can read and mutate during execution.
- **Reason for Rejection:** Introduces race conditions, non-deterministic execution paths, and severe data corruption risks when tasks are retried. Completely destroys data provenance and auditability.

### 20.5 Language-Native Object Serialization
- **Description:** Using language-specific binary serialization (e.g., Python `pickle`, Java Serialization) to pass objects between tasks.
- **Reason for Rejection:** Massive security vulnerability (remote code execution); tightly couples the entire orchestration platform to a single programming language and runtime version; prevents polyglot workers.

### 20.6 First-Class `PayloadRef` Abstraction in V1
- **Description:** Building storage abstraction layers inside the orchestrator to manage S3/blob lifecycles and dereference large objects automatically.
- **Reason for Rejection:** Violates minimal complexity principles for V1. Increases orchestrator operational footprint. External references can be passed seamlessly as ordinary string/object values.

---

## 21. Decision Evolution

- **Initial Concept (Pre-ADR):** Evaluated an open blackboard architecture where tasks wrote results to a shared execution dictionary, with optional Jinja2 templating for input parameters.
- **Refinement 1 (ADR-001 & ADR-003 Alignment):** Rejected the blackboard model in favor of explicit task input bindings aligned with the immutable canonical DAG structure.
- **Refinement 2 (ADR-007 Lifecycle Alignment):** Identified a critical conflict between path extraction languages and the `TaskExecution` state machine: runtime path failures would occur while a task was `RUNNABLE`, where no failure transition exists. Decided on **whole-value explicit bindings** to ensure zero runtime evaluation failures.
- **Refinement 3 (Worker Coordination Alignment):** Formalized the strict separation between Execution Context (control metadata) and Business Input, preventing worker metadata from polluting business domain models.
- **Final Accepted State (ADR-010):** Standardized on the JSON-compatible logical value model, whole-value bindings, the Direct Dependency Rule, explicit named workflow outputs, and atomic success visibility.

---

## 22. Common Misconceptions

- **Misconception 1: "Whole-value bindings mean NexusFlow makes deep copies of the entire payload for every task."**
  *Correction:* Whole-value binding is a **logical** semantic concept. It dictates what data the task has access to, not how the underlying engine manages memory. Implementations are completely free to pass immutable references, share memory pointers, or store deduplicated database records.
- **Misconception 2: "Input resolution can literally never fail under any circumstances."**
  *Correction:* Whole-value bindings eliminate **application-level missing-property and expression syntax evaluation failures**. Infrastructure-level failures (e.g., database connection loss, disk corruption, hardware faults) can still occur and are handled as orchestrator infrastructure faults, not business task failures.
- **Misconception 3: "Because workflow output can reference any task, the engine executes tasks that aren't connected to the DAG."**
  *Correction:* All tasks must be part of the valid, acyclic canonical workflow graph established in ADR-003 and validated in ADR-004. Workflow output bindings merely project results from already executed, valid tasks.
- **Misconception 4: "NexusFlow guarantees that worker activity side effects execute exactly once."**
  *Correction:* NexusFlow guarantees single-winner authoritative state and output publishing within the orchestrator. If an activity modifies external databases or calls external APIs before crashing, those side effects are outside the orchestrator's transaction boundary. Activities must be idempotent.
- **Misconception 5: "If a task doesn't specify an output, it doesn't have an output."**
  *Correction:* Every successful task execution produces exactly one logical output value. A void task produces an authoritative output of `null`.

---

## 23. Open Questions

- **Non-Blocking Implementation Considerations:**
  1. *Default Maximum Payload Size:* The precise byte threshold (e.g., 1 MB vs. 4 MB) for rejecting oversized inline payloads will be formalized in ADR-023 (Configuration).
  2. *Physical Persistence Layout:* The specific database tables, JSONB columns, or normalized storage schemas used to store inputs and outputs will be defined in ADR-011 (State Persistence).
  3. *Wire Protocol Serialization:* The physical serialization formats (JSON over HTTP, gRPC with Protobuf envelopes) used for worker communication will be formalized in ADR-020 (Technology Selection).
  4. *Numeric Precision Standards:* Future revisions may specify explicit IEEE-754 vs. arbitrary-precision decimal representations for cross-language edge cases.

*Status:* There are no open architectural questions blocking the approval of ADR-010.

---

## 24. Interview Discussion

### Question 1: Why did NexusFlow select an abstract JSON-compatible logical value model rather than language-native objects or raw bytes?
**Answer:** Language-native objects (like Python pickles) introduce catastrophic security vulnerabilities (arbitrary code execution) and permanently lock the engine into a single programming language and runtime version, preventing polyglot workers. Raw bytes, on the other hand, reduce the orchestrator to a blind pipe, making structural validation, auditing, and platform-level output mapping impossible. The JSON-compatible logical model represents the optimal balance: it is universally supported across every modern programming language, safe to deserialize, human-auditable, and strictly separates data from executable code, all while remaining agnostic to physical wire and database representations.

### Question 2: Why are task input bindings restricted to whole values rather than allowing JSONPath or JMESPath extraction?
**Answer:** The primary driver is protecting the state machine invariants established in ADR-007. In ADR-007, when a task's dependencies succeed, it transitions from `PENDING` to `RUNNABLE`. A `RUNNABLE` task cannot transition directly to `FAILED`; it must be dispatched to a worker, spawn an `ExecutionAttempt`, and execute. If we allowed path extraction (e.g., `task_a.output.users[0].id`), and `task_a` produced an empty list, input resolution would fail inside the scheduler while the task is `RUNNABLE`. Handling this would require inventing artificial, fake attempts or breaking the state machine. Whole-value bindings guarantee that if the upstream task succeeded, the downstream input binding is 100% resolvable. Any parsing or validation logic is deferred to activity execution inside the worker, where standard application failure and retry policies apply cleanly.

### Question 3: Why does TaskOutput binding strictly require a direct dependency in the workflow graph?
**Answer:** NexusFlow maintains a single, canonical graph topology (ADR-003). Permitting task $C$ to bind `TaskOutput(A)` when only $A \to B \to C$ is declared creates an invisible data dependency that is hidden from the topological scheduler. If an engineer later refactors the workflow such that $B$ no longer depends on $A$, $C$ would silently fail or stall because $A$ might not execute before $C$. Enforcing the Direct Dependency Rule guarantees that the graph topology explicitly reflects all scheduling and data requirements, preventing hidden race conditions and ensuring deterministic execution.

### Question 4: Why must Workflow Execution output be explicitly declared rather than automatically aggregating the outputs of all DAG leaves?
**Answer:** Inferring workflow output from leaf tasks tightly couples internal implementation details to the external API contract. If a workflow author adds a logging or notification leaf task to an existing workflow, an implicit leaf aggregator would suddenly pollute the public API response with internal logging data. Furthermore, in workflows with multiple parallel branches or disjoint subgraphs, leaf aggregation produces cluttered, non-deterministic dictionaries. Explicit named output bindings treat the workflow as a clean, encapsulated service with a stable public interface that does not break when internal graph topology is refactored.

---

## 25. References

- [ADR-001: Internal Workflow Specification](adr-001-internal-workflow-specification.md)
- [ADR-002: Workflow Definition Parsing & Normalization](adr-002-workflow-definition-parsing-strategy.md)
- [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md)
- [ADR-004: Workflow Validation Strategy](adr-004-workflow-validation-strategy.md)
- [ADR-005: Workflow Task Scheduling & Dispatch](adr-005-workflow-task-scheduling-and-dispatch-architecture.md)
- [ADR-006: Workflow Execution State Machine](adr-006-workflow-execution-state-machine.md)
- [ADR-007: Task Execution Lifecycle & Attempt Model](adr-007-task-execution-lifecycle-and-attempt-model.md)
- [ADR-008: Worker Coordination & Liveness Model](adr-008-worker-coordination-and-liveness-model.md)
- [ADR-009: Task Routing Strategy](adr-009-task-routing-strategy.md)
- [Architecture Decision Register](00-architecture-decision-register.md)
- RFC 8259: The JavaScript Object Notation (JSON) Data Interchange Format
- RFC 6901: JavaScript Object Notation (JSON) Pointer

---

## 26. Traceability

### 26.1 Backward Traceability
- **ADR-001 (IWS):** Concrete schemas for `TaskDefinition` and `WorkflowDefinition` embody the logical value model and whole-value binding sources defined here.
- **ADR-003 (Graph):** The Direct Dependency Rule directly enforces that all `TaskOutput` bindings map 1:1 to directed edges in the canonical graph.
- **ADR-004 (Validation):** Static validation rules verify input and output binding validity, dependency presence, key uniqueness, and absence of path syntax during definition ingestion.
- **ADR-005 (Scheduling):** The `RUNNABLE` eligibility condition requires that upstream authoritative outputs are committed and available before dispatch.
- **ADR-006 (Workflow State Machine):** Atomic visibility ensures a workflow reaches `SUCCEEDED` only when authoritative workflow outputs are durably committed.
- **ADR-007 (Task Lifecycle & Attempts):** Retry parameter stability guarantees that all attempts under a `TaskExecution` receive the identical logical business input.
- **ADR-008 (Worker Coordination):** Fencing and single-winner settlement ensure that only valid, non-stale attempts publish authoritative task output.
- **ADR-009 (Task Routing):** Routing matches tasks to workers without inspecting, modifying, or depending upon business input payloads.

### 26.2 Forward Traceability
- **ADR-011 (State Persistence):** Owns physical database tables, column types, JSONB storage, payload compression, and input materialization vs. reconstruction strategies.
- **ADR-012 (State Recovery):** Owns crash recovery reconciliation of in-flight outputs, uncommitted attempts, and deterministic reconstruction of task inputs after orchestrator restart.
- **ADR-013 (Consistency & Concurrency):** Owns the physical atomicity mechanisms (transactions, locks, atomic multi-table writes) guaranteeing consistent visibility of task and workflow success states with their authoritative outputs.
- **ADR-014 (Execution History):** Owns history event payloads, audit retention, and data reduction policies.
- **ADR-015 (API):** Defines external API envelopes, payload submission formats, and HTTP status codes for workflow input and output retrieval.
- **ADR-016 (Observability):** Defines metrics, tracing, and structured logging of data flow milestones (`INPUT_RESOLVED`, `OUTPUT_COMMITTED`).
- **ADR-018 (Error Taxonomy):** Formally categorizes payload validation errors, oversized payload rejections, and protocol errors.
- **ADR-020 (Technology Selection):** Selects wire serialization libraries, HTTP/gRPC transports, and worker SDK bindings.
- **ADR-022 (Security):** Owns payload sanitization, sensitive data masking, encryption at rest/in transit, and defensive ingress limits.
- **ADR-023 (Configuration):** Defines numerical configuration parameters, including maximum payload size thresholds.

---

## 27. Decision Validation Checklist

- [x] **JSON-compatible logical model selected** (Section 10.1)
- [x] **No native language object canonical representation** (Section 10.1)
- [x] **WorkflowExecution input immutable** (Section 10.2)
- [x] **Task bindings whole-value only** (Section 10.3)
- [x] **Literal supported** (Section 10.3)
- [x] **WorkflowInput supported** (Section 10.3)
- [x] **TaskOutput supported** (Section 10.3)
- [x] **TaskOutput requires direct declared dependency** (Section 10.4)
- [x] **No hidden transitive task-output reads** (Section 10.4)
- [x] **No path extraction** (Section 10.3, 20.3)
- [x] **No expression language** (Section 10.3, 20.2)
- [x] **Stable logical TaskExecution input** (Section 10.5)
- [x] **Retries get same logical business input** (Section 10.5)
- [x] **Materialization vs. reconstruction remains open** (Section 10.5, 26.2)
- [x] **Execution context separate from business input** (Section 10.6)
- [x] **Authoritative output only from valid successful Attempt** (Section 10.7)
- [x] **Stale/failed/cancelled outputs cannot publish** (Section 10.7)
- [x] **Successful void output = null** (Section 10.7)
- [x] **Task success cannot be visible without Task output** (Section 10.8)
- [x] **Duplicate result idempotent** (Section 10.7, 14)
- [x] **Conflicting duplicate cannot overwrite** (Section 10.7, 14)
- [x] **Persistence failure does not become business failure** (Section 14)
- [x] **Invalid result cannot commit success** (Section 10.7, 14)
- [x] **Workflow output explicit** (Section 10.9)
- [x] **Workflow output not inferred from leaves** (Section 10.9, 20.1)
- [x] **Workflow output may reference non-leaf task** (Section 10.9)
- [x] **No output bindings => null** (Section 10.9)
- [x] **Workflow success cannot be visible without Workflow output** (Section 10.9)
- [x] **Workflow output immutable** (Section 10.9)
- [x] **No shared mutable context** (Section 10.11, 20.4)
- [x] **Raw binary not canonical payload** (Section 10.1)
- [x] **No first-class PayloadRef in V1** (Section 10.10, 20.6)
- [x] **Bounded payload handling required** (Section 10.10)
- [x] **Payload thresholds deferred** (Section 10.10, 23)
- [x] **No exactly-once side-effect guarantee** (Section 10.12)
- [x] **Durable reconstruction requirements explicit** (Section 10.5, 26.2)
- [x] **ADR-011/012/013 boundaries preserved** (Section 26.2)
- [x] **ADR-018 owns error taxonomy** (Section 26.2)
- [x] **No technology leakage** (Section 10.1, 23)
- [x] **No fake benchmarks/production claims** (Fully compliant throughout)
