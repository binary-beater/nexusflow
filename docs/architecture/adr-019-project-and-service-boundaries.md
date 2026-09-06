# ADR-019 — Project & Service Boundaries

* **Status**: Approved — Not Frozen
* **Last Updated**: 2026-09-06
* **Domain**: Cross-Cutting & Governance
* **Criticality**: Supporting
* **Relationships**:
  * **Builds On**: [ADR-001](adr-001-internal-workflow-specification.md), [ADR-002](adr-002-workflow-definition-parsing-strategy.md), [ADR-003](adr-003-canonical-workflow-graph-representation.md), [ADR-004](adr-004-workflow-validation-strategy.md), [ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md), [ADR-006](adr-006-workflow-execution-state-machine.md), [ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md), [ADR-008](adr-008-worker-coordination-and-liveness-model.md), [ADR-009](adr-009-task-routing-strategy.md), [ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md), [ADR-011](adr-011-state-persistence-strategy.md), [ADR-012](adr-012-recovery-strategy.md), [ADR-013](adr-013-consistency-and-concurrency-strategy.md), [ADR-014](adr-014-execution-history-and-audit-model.md), [ADR-015](adr-015-external-api-architecture.md), [ADR-016](adr-016-observability-architecture.md), [ADR-017](adr-017-graceful-shutdown-architecture.md), [ADR-018](adr-018-error-handling-philosophy.md)
  * **Informs**: [ADR-020](00-architecture-decision-register.md), [ADR-021](00-architecture-decision-register.md), [ADR-022](00-architecture-decision-register.md), [ADR-023](00-architecture-decision-register.md), [ADR-025](00-architecture-decision-register.md), [ADR-026](00-architecture-decision-register.md)

---

## 1. Purpose

This Architectural Decision Record (ADR) establishes the project modularity, deployable boundaries, runtime component decomposition, dependency rules, transaction scoping, and repository organization strategy for the NexusFlow workflow orchestration engine.

The core architectural decision established by this record is:

> **Logical Modularity First, Distributed Boundaries Only Where Semantics Require Them.**  
> **NexusFlow V1 adopts a Modular Monolith Control Plane with an External Distributed Worker Runtime.**

This record establishes the boundary guarantees, interaction flows, layer dependencies, state ownership models, and future extraction criteria required to keep the system correct, decoupled, testable, and operationally maintainable.

---

## 2. Context

NexusFlow coordinates workflow definitions, execution state machines, task routing, worker liveness, atomic state persistence, recovery, execution audit history, and external API ingress:
* Ingesting workflow definitions, parsing ASTs ([ADR-002](adr-002-workflow-definition-parsing-strategy.md)), validating semantic DAG rules ([ADR-004](adr-004-workflow-validation-strategy.md)), and projecting canonical execution graphs ([ADR-003](adr-003-canonical-workflow-graph-representation.md)).
* Managing `WorkflowExecution` progression ([ADR-006](adr-006-workflow-execution-state-machine.md)), task readiness ([ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md)), attempt lifecycles ([ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md)), and data bindings ([ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md)).
* Tracking worker sessions and heartbeat liveness ([ADR-008](adr-008-worker-coordination-and-liveness-model.md)), matching runnable tasks against worker capabilities ([ADR-009](adr-009-task-routing-strategy.md)), and executing callbacks.
* Persisting state and history atomically ([ADR-011](adr-011-state-persistence-strategy.md), [ADR-014](adr-014-execution-history-and-audit-model.md)), enforcing optimistic concurrency fencing ([ADR-013](adr-013-consistency-and-concurrency-strategy.md)), executing startup recovery reconciliation ([ADR-012](adr-012-recovery-strategy.md)), managing graceful shutdown ([ADR-017](adr-017-graceful-shutdown-architecture.md)), and mapping errors cleanly across boundaries ([ADR-018](adr-018-error-handling-philosophy.md)).

Distributed systems face two failure-prone extremes:
1. **The Unstructured Monolith**: Application concerns intermingle without modular discipline. Ingress handlers execute persistence queries directly, state machine logic couples to external transport frameworks or drivers, and arbitrary external task execution runs in the same process memory as scheduling. This leads to fragility, high coupling, and process-wide failures when task code encounters unhandled execution faults.
2. **Premature Microservice Decomposition**: Control-plane subsystems are partitioned across multiple network services (e.g., separate network services for API, Parsing, Scheduling, Routing, Recovery, and History), each backed by an isolated database. Splitting consistency-coupled orchestration mutations across independently authoritative persistence boundaries introduces distributed coordination complexity, network failure modes, and serialization overhead, making atomic multi-entity consistency guarantees materially harder to preserve.

NexusFlow requires an explicit architectural boundary model that delivers strong domain encapsulation, strict failure containment, and clean testability while maintaining operational simplicity and atomic consistency.

---

## 3. Problem Statement

How should NexusFlow structure its project boundaries, deployable units, domain modules, infrastructure adapters, and worker runtimes so that:
1. The engine provides strict domain encapsulation, testability, and clear separation of concerns without introducing unnecessary distributed services or distributed consistency coordination complexity in V1?
2. External user tasks execute in isolated runtime environments with distinct failure containment without jeopardizing control-plane stability?
3. The control plane remains the sole authoritative orchestration state authority while workers act strictly as reporters of observations and results?
4. Multi-entity consistency groups (e.g., Attempt ownership commit, task settlement + output + history commit) execute atomically within one logical persistence boundary?
5. Dependencies flow strictly inward toward core domain semantics and port abstractions, isolating business logic from external frameworks, storage drivers, and transport protocols?
6. The codebase remains ergonomic, maintainable, and deployable for a solo developer or small team in V1 while preserving a clean path for selective service extraction in future versions?

---

## 4. Requirements Covered

### 4.1 Functional Requirements
* **FR-BND-001 (Control Plane Deployment Boundary)**: The control plane must build and deploy as a single deployable control-plane unit/process/artifact in V1.
* **FR-BND-002 (Worker Runtime Deployment Boundary)**: The worker runtime must build and deploy as an independent runtime capable of executing remotely from the control plane across network boundaries.
* **FR-BND-003 (Authoritative Domain Separation)**: Authoritative state mutations must be restricted exclusively to the control plane; workers must report observations, heartbeats, and results via explicit worker protocol contracts without direct access to authoritative orchestration persistence.
* **FR-BND-004 (In-Process Module Topography)**: The control plane must be partitioned into cohesive logical responsibilities with explicit in-process interfaces and zero internal network hops.
* **FR-BND-005 (Atomic Persistence Boundary)**: All multi-entity state mutations within a consistency group (such as Attempt ownership or state+history commits) must commit within one logical authoritative persistence boundary.
* **FR-BND-006 (Dependency Inversion Architecture)**: Core domain models, state machines, and validation rules must have zero dependencies on web frameworks, storage drivers, or transport layers.
* **FR-BND-007 (Shared Protocol Contracts)**: The interaction between the control plane and worker runtimes must be governed by an explicit, versionable, technology-neutral worker protocol specification.

### 4.2 Non-Functional Requirements
* **NFR-BND-001 (Operational Simplicity)**: The V1 control plane must run without mandatory distributed message brokers or distributed two-phase commit (2PC) protocols across internal modules, ensuring ease of local development, testing, and deployment.
* **NFR-BND-002 (Blast Radius & Failure Isolation)**: Task execution crashes, leaks, or unhandled failures within worker processes must be physically separated from the control-plane process memory.
* **NFR-BND-003 (Testability & Decoupling)**: Domain state machines and scheduling algorithms must be independently verifiable in-memory without requiring external storage instances or live network listeners.
* **NFR-BND-004 (Extensibility & Future Extraction)**: Module boundaries must be cleanly factored so that individual non-authoritative subsystems (e.g., read-side history projections or definition authoring tooling) can be extracted into network services in V2+ without re-architecting domain logic.

---

## 5. Constraints

1. **No Distributed Transactions Across V1 Control-Plane Modules**: In accordance with [ADR-011](adr-011-state-persistence-strategy.md) and [ADR-013](adr-013-consistency-and-concurrency-strategy.md), multi-entity operations (such as atomic Attempt ownership claims and state+history updates) must not span multiple independent authoritative persistence systems or require distributed two-phase commit protocols across control-plane subsystems in V1.
2. **State Machine Inviolability**: Structural modularity must not alter, bypass, or invent states in [ADR-006](adr-006-workflow-execution-state-machine.md) (`INITIALIZING`, `RUNNING`, `FAILING`, `FAILED`, `CANCELLING`, `CANCELLED`, `SUCCEEDED`), [ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md) (`PENDING`, `RUNNABLE`, `RUNNING`, `RETRY_WAIT`, `SUCCEEDED`, `FAILED`, `CANCELLED`, and attempts `CLAIMED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`), or [ADR-008](adr-008-worker-coordination-and-liveness-model.md).
3. **First-Valid-Authoritative-Winner Integrity**: Worker callback processing and concurrency fencing must strictly obey [ADR-013](adr-013-consistency-and-concurrency-strategy.md); stale callbacks or superseded attempts must be rejected without mutating durable state.
4. **Technology Neutrality**: ADR-019 must not select concrete programming languages, web routers, ORMs, wire transports, storage dialects, telemetry frameworks, or process supervisors (deferred to [ADR-020](00-architecture-decision-register.md)).
5. **No Authoring Format Proliferation in V1**: External workflow authoring in V1 is YAML only ([ADR-002](adr-002-workflow-definition-parsing-strategy.md)).

---

## 6. Goals

* Establish the deployable boundaries for NexusFlow V1 (Control Plane vs. External Worker Runtime).
* Define the cohesive logical responsibilities comprising the modular monolith control plane.
* Enforce an inward dependency direction (Ports and Adapters) separating domain semantics from external infrastructure.
* Formalize the state ownership matrix across durable persistence and ephemeral runtime state.
* Establish the application use-case layer as the coordinator between interfaces, domain semantics, and persistence ports.
* Define cross-module consistency groups and the single logical atomic persistence boundary.
* Distinguish repository organization from architectural boundaries, providing a defensible monorepo recommendation for V1 developer ergonomics.
* Establish clear, defensible criteria for potential service extraction in future versions (V2+).

---

## 7. Non-Goals

* **Selecting Concrete Languages & Frameworks**: Choosing programming languages, web frameworks, or storage drivers is deferred to [ADR-020](00-architecture-decision-register.md).
* **Selecting Concrete Wire Protocols**: Choosing between HTTP/REST, gRPC, or WebSockets for worker transport is deferred to [ADR-020](00-architecture-decision-register.md).
* **Specifying Storage Schemas & Dialects**: Specifying table schemas, storage dialects, indexing, and migration tooling is deferred to [ADR-020](00-architecture-decision-register.md).
* **Designing Multi-Orchestrator Consensus**: Clustering, leader election, and distributed consensus mechanisms are deferred to [ADR-025](00-architecture-decision-register.md).
* **Designing Worker SDKs**: Language-specific worker libraries and client generation tools are deferred to [ADR-026](00-architecture-decision-register.md).
* **Freezing Directory Trees or Physical Package Counts**: ADR-019 governs logical architecture, not exact repository directories or packaging counts.

---

## 8. Candidate Solutions

### Candidate A: Unstructured Monolith
All components (ingress API, definition parser, execution domain, scheduler, persistence adapters, and task execution logic) reside within a single codebase, package, and runtime process without strict boundary enforcement. Task code executes within the same process address space as the orchestrator.

* *Pros*: Minimal initial setup overhead; direct in-memory calls everywhere; zero network serialization.
* *Cons*: Complete lack of failure containment; task execution crashes, memory leaks, or unhandled failures directly terminate the control plane; high coupling makes domain logic difficult to test independently; unable to support heterogeneous worker environments cleanly.

### Candidate B: Fine-Grained Microservices
Every major domain area is deployed as an independent network service with an isolated persistence store (e.g., separate network services for API, Definition, Scheduler, Routing, Worker Coordination, Recovery, and History).

* *Pros*: Independent process scaling per service; physical module boundaries enforced by network firewalls; isolated team ownership.
* *Cons*: Splitting consistency-coupled orchestration mutations across independently authoritative persistence boundaries introduces distributed coordination and consistency complexity, making atomic multi-entity consistency guarantees materially harder to preserve; introduces network serialization latency, partial failure modes, and substantial operational overhead with no established requirement in V1.

### Candidate C: Modular Monolith Control Plane + External Distributed Worker Runtime (SELECTED)
The control plane is organized into cohesive logical modules communicating via in-process semantic interfaces, deployed as a single deployable unit backed by one logical authoritative persistence boundary. Workers execute as independent external runtimes across an explicit worker protocol boundary.

* *Pros*: Clean domain encapsulation and testability; atomic multi-entity consistency without distributed coordination complexity; operational simplicity; strong failure containment for external task execution; clean path for future selective service extraction.
* *Cons*: Control-plane components scale together in V1; discipline required to prevent internal module coupling.

### Candidate D: Modular Monolith with Internal Event Bus / Choreography
Control-plane modules are contained within a single process but communicate primarily through an asynchronous in-process publish-subscribe event bus (or broker).

* *Pros*: Decoupled event-driven choreography; easy addition of secondary event listeners.
* *Cons*: Authoritative orchestration progression must not depend on ephemeral events; coordinating state mutation with event publication introduces dual-write and consistency concerns depending on implementation; asynchronous choreography complicates deterministic orchestration reasoning.

---

## 9. Detailed Evaluation

| Architectural Criterion | Candidate A: Unstructured Monolith | Candidate B: Fine-Grained Microservices | Candidate C: Modular Monolith + Dist. Workers (Selected) | Candidate D: Monolith + Internal Event Bus |
| :--- | :--- | :--- | :--- | :--- |
| **Failure Containment (NFR-BND-002)** | None; worker task faults crash orchestrator | High; service crashes are physically isolated | **High; worker execution physically separated from control plane process memory** | Medium; worker isolated if external, but bus failures stall core |
| **Transactional Atomicity (FR-BND-005)** | High; local persistence | Very Low; requires distributed coordination or sagas | **High; one logical atomic persistence boundary** | Low; state persistence and event publication coordination hazards |
| **Operational Simplicity (NFR-BND-001)** | Very High; single process | Very Low; numerous services, networks, and storage stores | **High; single deployable control-plane unit** | Medium; event bus/queue lifecycle and subscription management |
| **Domain Decoupling & Testability (NFR-BND-003)** | Very Low; tangled dependencies | High; forced network interfaces | **High; clean port abstractions and in-memory testability** | Medium; asynchronous event choreography complicates testing |
| **Correctness & Liveness Integrity** | Low; task failure corrupts memory | Low; distributed race conditions & partial network failures | **High; sole authoritative control plane + ADR-013 fencing** | Low; ephemeral event loss risks stalled orchestration progression |
| **Solo Developer Ergonomics** | High initially, very low later | Extremely Low; massive operational maintenance overhead | **High; rapid iteration and straightforward debugging** | Medium; tracing asynchronous event choreography is burdensome |

---

## 10. Decision

### 10.1 Central Decision Statement
**NexusFlow V1 adopts a Modular Monolith Control Plane with an External Distributed Worker Runtime.**

The control plane is deployed as a single deployable unit/process/artifact in V1, organized into cohesive logical responsibilities communicating through in-process semantic interfaces. Authoritative multi-entity state changes coordinate through one logical persistence boundary capable of preserving [ADR-011](adr-011-state-persistence-strategy.md), [ADR-013](adr-013-consistency-and-concurrency-strategy.md), and [ADR-014](adr-014-execution-history-and-audit-model.md) consistency groups.

Workers are separately deployable runtimes communicating across an explicit worker protocol boundary. They execute task logic in a worker-local runtime environment and report observations and results, but never directly mutate authoritative orchestration state.

Application and domain logic depend inward on semantic ports rather than outward on concrete infrastructure. Concrete storage implementations, worker wire protocols, runtime models, persistence abstractions, telemetry technologies, and process mechanics are deferred to [ADR-020](00-architecture-decision-register.md).

Repository organization is explicitly distinct from module, process, and service boundaries. A monorepo is recommended for V1 developer ergonomics but is not an architectural correctness requirement.

### 10.2 Boundary & Terminology Formalization
To prevent architectural confusion, NexusFlow formalizes these four terms:

$$\text{Repository Boundary} \neq \text{Module Boundary} \neq \text{Process Boundary} \neq \text{Service Boundary}$$

1. **Logical Module**: A cohesive boundary of domain responsibilities, business invariants, and semantic ports (e.g., Definition Management, Execution Domain). In-process and non-distributed.
2. **Deployable Process / Unit**: An independently packaged and executed operating system entity (e.g., the Control Plane unit, a Worker process).
3. **Network Service**: A deployable process exposing an addressable network interface across network boundaries. The only NexusFlow-owned semantic runtime distribution boundary required in V1 is **Control Plane $\longleftrightarrow$ Worker Runtime**. External persistence and telemetry backends may also be remote dependencies, but they are external infrastructure dependencies, not independently authoritative NexusFlow orchestration services.
4. **Repository**: A version-controlled code storage boundary. An organizational choice for developer ergonomics, not a runtime architecture invariant.

```
+-----------------------------------------------------------------------------------+
|                        NEXUSFLOW CONTROL PLANE                                    |
|             (Single Deployable Unit / Process / Artifact in V1)                   |
|                                                                                   |
|  [Interfaces] -> [Application Use Cases] -> [Domain Semantics]                    |
|                                                     |                             |
|                                                     v                             |
|                                              [Semantic Ports]                     |
|                                                     ^                             |
|                                                     | (Implements)                |
|                                       [Infrastructure Adapters]                   |
+-----------------------------------------------------------------------------------+
       |                                     |                         |
       | Worker Protocol                     | Persistence Port        | Telemetry Port
       | (Network Boundary)                  | (Network / Local IO)    | (Network / Local IO)
       v                                     v                         v
+-----------------------+          +-------------------+     +--------------------+
| WORKER RUNTIME        |          | External Storage  |     | External Telemetry |
| (Separately           |          | Infrastructure    |     | Infrastructure     |
| Deployable Processes) |          | (Non-Service Dep) |     | (Non-Service Dep)  |
+-----------------------+          +-------------------+     +--------------------+
```

### 10.3 Control-Plane Logical Responsibilities
The control plane is organized into eight cohesive logical responsibilities (illustrative, not frozen physical package counts or class names):
1. **Interfaces**: Public ingress ([ADR-015](adr-015-external-api-architecture.md)) and Worker Protocol adapters. External authoring ingestion is YAML only ([ADR-002](adr-002-workflow-definition-parsing-strategy.md)). Translates external transport requests into application use cases. Interfaces never bypass the application layer to mutate persistence directly.
2. **Definition Management**: Ingestion, YAML parsing, normalization to Candidate IWS ([ADR-002](adr-002-workflow-definition-parsing-strategy.md)), validation pipeline ([ADR-004](adr-004-workflow-validation-strategy.md)), and canonical graph projection ([ADR-003](adr-003-canonical-workflow-graph-representation.md)).
3. **Execution Domain**: Core business invariants and state machines for `WorkflowExecution` ([ADR-006](adr-006-workflow-execution-state-machine.md)), `TaskExecution`, and `ExecutionAttempt` ([ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md)), as well as parameter data-flow evaluation ([ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md)) and semantic HistoryEntry construction ([ADR-014](adr-014-execution-history-and-audit-model.md)).
4. **Orchestration**:
   * *Scheduler*: Evaluates task dependency satisfaction, identifies runnable tasks, and triggers eligibility transitions per [ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md). Operates directly on execution state without separate durable correctness state. No quotas or concurrency caps in V1.
   * *Routing*: Evaluates candidate matching relation based on live `WorkerSession`, `accepting_new_work = true`, and exact canonical Activity Type compatibility ([ADR-009](adr-009-task-routing-strategy.md)). Candidate offers are ephemeral and advisory.
   * *Worker Coordination*: Manages authoritative `WorkerSession` lifecycle, liveness evaluation, and callback correlation ([ADR-008](adr-008-worker-coordination-and-liveness-model.md)).
5. **Recovery**: Startup reconciliation, targeted runtime reconciliation, and current-state inspection per [ADR-012](adr-012-recovery-strategy.md). Uses standard domain transition write paths with [ADR-013](adr-013-consistency-and-concurrency-strategy.md) concurrency guards.
6. **Persistence Boundary**: Explicit semantic persistence ports and capabilities supporting atomic multi-entity consistency groups ([ADR-011](adr-011-state-persistence-strategy.md), [ADR-013](adr-013-consistency-and-concurrency-strategy.md), [ADR-014](adr-014-execution-history-and-audit-model.md)).
7. **Runtime / Composition**: Application composition root, wiring concrete infrastructure adapters to semantic ports, managing process lifecycle/drain ([ADR-017](adr-017-graceful-shutdown-architecture.md)), and providing the abstract control-plane time capability.
8. **Observability Integration**: Cross-cutting instrumentation providing metrics, logging, and tracing adapters ([ADR-016](adr-016-observability-architecture.md)). Best-effort, non-authoritative, and fail-open.

### 10.4 Conceptual Application Layer
NexusFlow explicitly incorporates a conceptual Application / Use-Case layer that coordinates between outer interfaces and inner domain models. Representative semantic use cases include:
* `RegisterDefinition`: Coordinates YAML parsing, validation, and durable definition registration.
* `StartExecution`: Coordinates definition lookup, workflow entity creation, initial task establishment, and atomic commit.
* `CancelExecution`: Evaluates guarded workflow cancellation transitions and records audit history.
* `CommitAttemptOwnership`: Coordinates task eligibility revalidation, attempt binding, and atomic ownership commit.
* `ProcessWorkerResult`: Coordinates callback authority verification, result evaluation, output structural validation, and atomic settlement commit.
* `ReconcileExecution`: Coordinates recovery inspection, candidate state re-evaluation, and corrective state commits.

### 10.5 Dependency Direction (Ports and Adapters)
Dependencies flow strictly inward toward core domain semantics:

```
Interfaces / Runtime Entrypoints
              |
              v
    Application Use Cases
              |
              v
       Domain Semantics
              |
              v (depend on)
       Semantic Ports
              ^
              | (implement)
   Infrastructure Adapters
```

* **Core Domain Independence**: Domain models, state machines, and port abstractions have zero dependencies on external transport routers, serialization frameworks, storage drivers, or wire protocols.
* **Ports as Boundaries**: Application use cases interact with persistence, external communication, and clocks through semantic port interfaces.
* **Adapters Depend Inward**: Concrete infrastructure adapters (storage engines, protocol listeners, telemetry emitters) implement port interfaces and depend inward on domain contracts. Adapters are composed and wired at the runtime composition root.

### 10.6 State Ownership Matrix

| Subsystem | State Item | Durability & Authority Model |
| :--- | :--- | :--- |
| **Definition Management** | Definition Truth | **Validated IWS is authoritative durable semantic truth** ([ADR-001](adr-001-internal-workflow-specification.md), [ADR-011](adr-011-state-persistence-strategy.md)). The **Canonical Graph is derived/reconstructible** ([ADR-003](adr-003-canonical-workflow-graph-representation.md)). AST metadata and provenance are diagnostic; persistence is optional and not mandated. |
| **Execution Domain** | Workflow Execution | `WorkflowExecution` is **durable and authoritative** ([ADR-006](adr-006-workflow-execution-state-machine.md), [ADR-011](adr-011-state-persistence-strategy.md)). |
| **Execution Domain** | Task Execution | `TaskExecution` is **durable and authoritative** ([ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md), [ADR-011](adr-011-state-persistence-strategy.md)). |
| **Execution Domain** | Execution Attempt | `ExecutionAttempt` is **durable and authoritative** ([ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md), [ADR-011](adr-011-state-persistence-strategy.md)). |
| **Data Flow** | Workflow Input | Immutable for an execution; durable upon acceptance ([ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md), [ADR-011](adr-011-state-persistence-strategy.md)). |
| **Data Flow** | Task Input | Stable for the `TaskExecution` across retries/Attempts ([ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md), [ADR-011](adr-011-state-persistence-strategy.md)); materialized durably or deterministically reconstructed. Not an immutable per-attempt payload. |
| **Data Flow** | Task Output | Authoritative output **belongs to the successful TaskExecution outcome** ([ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md)). Failed or superseded Attempt payloads are not authoritative outputs. |
| **Data Flow** | Workflow Output | Authoritative output exists **only upon successful terminalization (`SUCCEEDED`)** ([ADR-006](adr-006-workflow-execution-state-machine.md), [ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md)). None produced for `FAILED` or `CANCELLED`. |
| **Worker Coordination** | Worker Registry | **Ephemeral and reconstructible** via heartbeats ([ADR-008](adr-008-worker-coordination-and-liveness-model.md)). Physical storage (in-memory map, cache) deferred to ADR-020. |
| **Worker Coordination** | Session Correlation | Attempt-to-session association is **durable as part of execution state** ([ADR-008](adr-008-worker-coordination-and-liveness-model.md), [ADR-011](adr-011-state-persistence-strategy.md)). |
| **Routing** | Routing Candidates | **Ephemeral and advisory**. No persistent queues, ready queues, or worker waitlists exist. |
| **Scheduler** | Scheduling State | **No independent durable correctness state**. State derived from authoritative execution records ([ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md)). |
| **Recovery** | Recovery State | **No independent durable recovery state**. No checkpoint scans or cursors ([ADR-012](adr-012-recovery-strategy.md)). |
| **Audit & History** | Execution History | **Durable, append-oriented `HistoryEntry` stream** committed atomically with state ([ADR-014](adr-014-execution-history-and-audit-model.md)). Immutable while retained; non-authoritative for state progression. |
| **Observability** | Telemetry | **Non-authoritative and fail-open** ([ADR-016](adr-016-observability-architecture.md)). |
| **Runtime** | Drain & Admission | **Ephemeral control-plane lifecycle state** ([ADR-017](adr-017-graceful-shutdown-architecture.md)). `accepting_new_work` belongs strictly to `WorkerSession` ([ADR-008](adr-008-worker-coordination-and-liveness-model.md)). |

### 10.7 Multi-Entity Consistency Groups
NexusFlow requires one logical authoritative persistence boundary supporting atomic commits across multi-entity consistency groups:
* **Attempt Ownership**: Transition `TaskExecution` `RUNNABLE` $\to$ `RUNNING`, create `ExecutionAttempt` `CLAIMED`, bind `WorkerSessionId`, assign attempt ordinal, and record `HistoryEntry`.
* **Task Input Readiness**: Transition `TaskExecution` `PENDING` $\to$ `RUNNABLE` when dependencies are satisfied and inputs are resolved.
* **Task Success + Authoritative Output**: Transition `TaskExecution` and `ExecutionAttempt` to `SUCCEEDED`, commit validated task output, and record `HistoryEntry`.
* **Retry Scheduling**: Transition `ExecutionAttempt` to `FAILED`, transition `TaskExecution` to `RETRY_WAIT`, compute retry delay, and record `HistoryEntry`.
* **Definitive Task Failure**: Transition `ExecutionAttempt` to `FAILED`, transition `TaskExecution` to `FAILED`, evaluate workflow failure direction ([ADR-006](adr-006-workflow-execution-state-machine.md)), and record `HistoryEntry`.
* **Workflow Terminalization**: Transition `WorkflowExecution` to terminal state (`SUCCEEDED`, `FAILED`, `CANCELLED`), commit authoritative workflow output (if succeeded), and record `HistoryEntry`.
* **State + History Atomicity**: Every semantic lifecycle transition must commit alongside its corresponding audit record within the same atomic persistence boundary ([ADR-014](adr-014-execution-history-and-audit-model.md)).

### 10.8 Conceptual Sequence Workflows

#### 1. Definition Registration Flow
```
Client -> Interfaces: Submit Workflow YAML
Interfaces -> Application: RegisterDefinition(raw_yaml)
Application -> Definition: Parse & Normalize to Candidate IWS (ADR-002)
Application -> Definition: Validate Candidate IWS (ADR-004)
Definition -> Application: Validated IWS + Derived Canonical Graph (ADR-003)
Application -> Persistence Port: Register Definition (Validated IWS) (ADR-011)
Persistence Port -> Application: Definition Registered (DefinitionId)
Application -> Interfaces: Registration Result
Interfaces -> Client: 201 Created (DefinitionId)
```

#### 2. Execution Start Flow
```
Client -> Interfaces: Start Execution (DefinitionId, Input, optional opaque idempotency token)
Interfaces -> Application: StartExecution(...)
Application -> Persistence Port: Resolve exact DefinitionId & load Validated IWS
Application -> Domain: Construct WorkflowExecution in INITIALIZING and expected TaskExecution entities (ADR-006, ADR-007)
Application -> Persistence Port: Durably establish required initialization state/history (ADR-011, ADR-014)
Persistence Port -> Application: Initialization Committed
Application -> Domain: When ADR-006 initialization satisfied, propose guarded INITIALIZING -> RUNNING
Application -> Persistence Port: Commit transition to RUNNING (ADR-006, ADR-013)
Persistence Port -> Application: Committed
Application -> Interfaces: Execution Started (authoritative status: INITIALIZING or RUNNING)
Interfaces -> Client: 201 Created (ExecutionId, Status)
Application -> Optional Internal Trigger: Notify Scheduler of runnable work
```

#### 3. Task Ownership Flow
```
Scheduler -> Domain: Identify TaskExecution in RUNNABLE state (ADR-005)
Scheduler -> Routing: Query Candidate WorkerSession for exact Activity Type (ADR-009)
Routing -> Scheduler: Candidate Match (Ephemeral Offer)
Scheduler -> Application: CommitAttemptOwnership(TaskId, WorkerSessionId)
Application -> Domain: Revalidate:
                       - Workflow is RUNNING
                       - Task is RUNNABLE
                       - No active Attempt exists
                       - WorkerSession is live
                       - WorkerSession accepting_new_work == true
                       - Exact canonical Activity Type compatibility
Domain -> Application: Revalidation Succeeded
Application -> Persistence Port: Atomic Commit (Task RUNNING, Attempt CLAIMED, Ordinal, History) (ADR-013)
Persistence Port -> Application: Committed
Application -> Worker Protocol Adapter: Transmit Dispatch Task Message
```

#### 4. Result Processing Flow
```
Worker -> Worker Protocol Adapter: Transmit Task Result Message
Worker Protocol Adapter -> Worker Coordination: Validate (AttemptId, WorkerSessionId) Authority (ADR-008)
Worker Coordination -> Application: ProcessWorkerResult(AttemptId, Outcome, Output/Error)
Application -> Domain: Validate structural JSON-compatible output rules if SUCCEEDED (ADR-010)
Domain -> Domain: Resolve Task Settlement & Attempt Termination (ADR-007, ADR-018)
Application -> Persistence Port: Atomic Commit (Task + Attempt + Output + History) (ADR-011)
Persistence Port -> Application: Committed
Application -> Optional Internal Trigger: Notify Scheduler of downstream readiness
```

#### 5. Workflow Cancellation Flow
```
Client -> Interfaces: Cancel Execution Request
Interfaces -> Application: CancelExecution(WorkflowExecutionId)
Application -> Domain: Evaluate Guarded Workflow Transition (e.g., INITIALIZING/RUNNING -> CANCELLING) (ADR-006, ADR-013)
Application -> Persistence Port: Atomic Commit (Workflow State + History)
Persistence Port -> Application: Committed
Application -> Worker Protocol Adapter: Best-Effort Worker Cancellation Request (ADR-008)
```
*(Note: Repeated cancellations of an already CANCELLING/CANCELLED workflow follow ADR-015 idempotent behavior. CANCELLED is committed only after all tasks settle per ADR-006).*

#### 6. Recovery Flow
```
Recovery Startup / Trigger -> Persistence Port: Inspect Current Authoritative State (ADR-012)
Persistence Port -> Recovery: Active Workflows & In-Flight Attempts
Recovery -> Domain: Evaluate State Machine Rules & Liveness Status (ADR-006, ADR-007, ADR-008)
Domain -> Recovery: Required Corrective Transitions
Recovery -> Persistence Port: Guarded Atomic Transition Commits (ADR-013)
Recovery -> Scheduler: Trigger or permit normal scheduling re-evaluation as appropriate
```

---

## 11. Decision Rationale

1. **Elimination of Distributed Coordination Complexity Across Control-Plane Modules**: Confining the control plane to a single deployable unit backed by one logical persistence boundary allows multi-entity consistency groups to commit atomically within the single logical authoritative persistence boundary. This eliminates the need for distributed two-phase commit protocols or complex saga mechanics between internal control-plane modules in V1.
2. **Blast Radius & Failure Isolation**: External task execution runs user-defined code with unpredictable resource usage and potential unhandled failures. Deploying workers as separate external runtimes ensures that a worker-runtime process failure does not directly crash the control-plane process through shared process memory.
3. **Preservation of Orchestration Authority**: Confining authoritative state progression to the control plane prevents split-brain decision-making. Workers execute external task logic across a separate runtime boundary and are not trusted with authoritative orchestration mutation capability. They report observations and results that the control plane validates against [ADR-013](adr-013-consistency-and-concurrency-strategy.md) concurrency guards.
4. **Independent Worker Scalability**: Different tasks have distinct compute, memory, and environment profiles. External worker runtimes scale horizontally across independent nodes and heterogeneous environments without requiring control-plane re-architecture.
5. **Ergonomic Solo/Small Team Velocity**: A modular monolith minimizes continuous integration, deployment, and operational overhead while maintaining clear module boundaries. In-process interfaces and explicit dependency rules allow refactoring without coordinating distributed microservice deployment pipelines.

---

## 12. Tradeoffs

### 12.1 Benefits
* **Zero Internal Network Latency**: In-process semantic calls between control-plane modules incur no serialization overhead, socket churn, or network timeout handling.
* **Atomic Consistency Guarantees**: Multi-entity consistency groups (e.g., attempt ownership, state+history commits) execute with guaranteed atomicity within one logical persistence boundary.
* **Streamlined Developer Experience**: Local debugging, unit testing, and continuous integration do not require orchestrating complex multi-service container topologies.
* **Independent Testability**: Domain semantics should be independently testable without requiring real external infrastructure, using suitable port abstractions.

### 12.2 Costs & Mitigations
* **Coupled Control-Plane Scaling**: All control-plane modules scale together as a single process in V1.  
  * *Mitigation*: Compute-intensive user task execution is intentionally separated into worker runtimes, while the control plane remains responsible for orchestration semantics.
* **Discipline Required Against Architectural Erosion**: In-process modules risk leaking domain models or bypassing ports without strict discipline.  
  * *Mitigation*: The Application use-case layer strictly coordinates state mutations; ingress interfaces cannot call persistence directly; explicit dependency rules can later be reinforced through package/module structure, static analysis, linting, or architectural tests depending on the selected technology ([ADR-020](00-architecture-decision-register.md)/[ADR-021](00-architecture-decision-register.md)).
* **Shared Process Failure Domain**: An unhandled process crash in the control plane affects all internal modules simultaneously.  
  * *Mitigation*: Durable current state and [ADR-012](adr-012-recovery-strategy.md) startup reconciliation allow the control plane to recover authoritative orchestration progression after process restart under [ADR-012](adr-012-recovery-strategy.md), [ADR-013](adr-013-consistency-and-concurrency-strategy.md), and [ADR-018](adr-018-error-handling-philosophy.md) integrity rules.

---

## 13. Consequences

### 13.1 Positive Consequences
* Clear division of responsibility: The control plane decides; workers execute and report.
* Simplified operational footprint for V1: Single control-plane deployable unit + external workers + storage infrastructure.
* V1 does not require distributed coordination between independently authoritative control-plane services because the control plane remains one logical authority/persistence boundary.
* Future service extractions (e.g., read-side history projections) can occur incrementally without modifying core domain invariants.

### 13.2 Negative Consequences
* High-throughput read queries (e.g., extensive history audit queries) share the same logical persistence boundary as critical write transitions in V1.
* Control-plane language and runtime framework choices ([ADR-020](00-architecture-decision-register.md)) apply across all internal modules.

---

## 14. Failure Modes & Boundary Behavior

| Boundary Failure Scenario | Root Cause | System & Boundary Behavior | Invariant Maintained |
| :--- | :--- | :--- | :--- |
| **Parser / Validation Fault** | Malformed YAML syntax or invalid DAG structure | Contained within Definition Management; rejected at ingress; returns client error; no state persisted. | Execution domain protected from invalid definitions. |
| **Worker Disconnect Pre-Ownership** | Worker disconnects or offer becomes invalid before commit | If the candidate becomes invalid before ownership commit, revalidation prevents authoritative ownership from being established; Task remains `RUNNABLE`; no Attempt created; no retry budget or ordinal consumed. | [ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md) retry budget inviolability; [ADR-009](adr-009-task-routing-strategy.md). |
| **Worker Loss During Execution** | Worker process crash, machine failure, or heartbeat timeout | [ADR-008](adr-008-worker-coordination-and-liveness-model.md) determines `WorkerSession` liveness/loss. If authoritative worker-loss settlement wins while Attempt remains active, Attempt normally becomes `FAILED` unless another valid terminal outcome won first under [ADR-013](adr-013-consistency-and-concurrency-strategy.md). | [ADR-013](adr-013-consistency-and-concurrency-strategy.md) first-valid-winner semantics. |
| **Stale / Duplicate Result Report** | Worker sends result after attempt was superseded or timed out | Worker Coordination verifies `(AttemptId, WorkerSessionId)` authority and checks [ADR-013](adr-013-consistency-and-concurrency-strategy.md) OCC preconditions; stale callback is rejected; durable state remains unchanged. | Stale write fencing; [ADR-013](adr-013-consistency-and-concurrency-strategy.md). |
| **Persistence Unavailable** | Storage backend connection lost or read-only | Authoritative progression requiring durable persistence does not become committed merely from local/in-process computation; mutation fails closed; error normalized at persistence port; once authoritative persistence becomes available, subsequent processing or startup/targeted reconciliation follows [ADR-012](adr-012-recovery-strategy.md)/[ADR-013](adr-013-consistency-and-concurrency-strategy.md). | State consistency; no uncommitted state progression. |
| **Unknown Persistence Commit Outcome** | Network drop during commit acknowledgement | Application layer treats outcome as unknown; relies on [ADR-013](adr-013-consistency-and-concurrency-strategy.md) authoritative state reread before retrying mutation. | At-most-once transition progression. |
| **Scheduler Operation Interruption** | Local scheduling operation failure or process restart | A local scheduling operation failure may be retried or re-evaluated according to [ADR-018](adr-018-error-handling-philosophy.md); a process crash/restart invokes [ADR-012](adr-012-recovery-strategy.md) startup reconciliation; durable current state remains correctness authority. | [ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md), [ADR-012](adr-012-recovery-strategy.md) recovery correctness. |
| **Telemetry Pipeline Stall / Loss** | Telemetry collector offline or buffer full | Telemetry failure is contained and fail-open with respect to orchestration correctness; telemetry may be dropped, sampled, buffered, or otherwise handled according to [ADR-020](00-architecture-decision-register.md)/[ADR-023](00-architecture-decision-register.md) implementation policy; core state transitions commit unimpeded. | Telemetry is strictly non-authoritative ([ADR-016](adr-016-observability-architecture.md)). |

---

## 15. Debugging & Observability Across Boundaries

* **Clear Failure Attribution**: The boundary between control-plane orchestration and external worker reporting isolates root causes. Worker execution failures appear as failed attempts with failure details ([ADR-018](adr-018-error-handling-philosophy.md)), distinct from control-plane scheduling errors.
* **Correlated Context Tracing**: Observability should include applicable semantic correlation identifiers such as `WorkflowExecutionId`, `TaskExecutionId`, `AttemptId`, `WorkerSessionId`, request correlation, and trace/span correlation where relevant per [ADR-016](adr-016-observability-architecture.md).
* **Inspectable Authoritative State**: Authoritative execution state can be inspected without relying on hidden durable scheduler or recovery state. History supports semantic audit, and telemetry supports operational diagnostics.
* **Port-Level Diagnostics**: Infrastructure adapter failures are normalized at architectural boundaries according to [ADR-018](adr-018-error-handling-philosophy.md) and may produce appropriate best-effort diagnostic telemetry according to [ADR-016](adr-016-observability-architecture.md).

---

## 16. Testing Strategy

ADR-019 establishes explicit architectural boundaries that enable a layered verification strategy (concrete testing strategy and tooling deferred to [ADR-021](00-architecture-decision-register.md)):
1. **Domain Unit Testing (Pure In-Memory)**: Core state machines (`WorkflowExecution`, `TaskExecution`, `ExecutionAttempt`), DAG validation algorithms, and routing logic must be independently verifiable in-memory without requiring live storage or network services.
2. **Application Use-Case Testing (Port Abstractions)**: Application use cases (`RegisterDefinition`, `StartExecution`, `CommitAttemptOwnership`, `ProcessWorkerResult`) should be verified using test doubles, fakes, or suitable semantic port implementations.
3. **Cross-Module Consistency Verification**: Integration tests should verify that atomic multi-entity consistency groups (e.g., Attempt ownership, state+history commits) commit atomically through real storage adapters.
4. **Boundary Violation Tests**:
   * Should verify that workers cannot mutate state directly or report callbacks with mismatched/stale session identities.
   * Should verify that public API routes cannot invoke persistence ports directly, bypassing the Application layer.
   * Should verify that pre-ownership worker disconnections leave tasks `RUNNABLE` without consuming attempt ordinals or retry budgets.
   * Should verify that telemetry failures do not impede or fail authoritative persistence transactions.
5. **Contract Conformance Testing**: Should verify that both the control plane and worker runtimes adhere strictly to the shared worker protocol specification.

---

## 17. Operational Considerations

* **Deployment Footprint**: The V1 control plane deploys as a single operational unit/process alongside a supported persistence backend. Workers deploy independently across target worker nodes.
* **Capacity Planning**: Control-plane sizing is driven by workflow metadata churn and transaction throughput. Worker fleet sizing is driven by user task execution characteristics (CPU, memory, GPU, I/O).
* **Process Lifecycle & Draining**: During shutdown, the control plane enters `DRAINING` ([ADR-017](adr-017-graceful-shutdown-architecture.md)). `DRAINING` is a conceptual ephemeral process lifecycle and admission state, not a `WorkflowExecution`, `TaskExecution`, or `Attempt` domain state. Per [ADR-017](adr-017-graceful-shutdown-architecture.md):
  * The control plane closes admission for new external mutations according to V1 shutdown policy.
  * The orchestrator stops establishing new scheduler ownership after the drain boundary.
  * Already-admitted authoritative operations receive a bounded opportunity to settle.
  * Active Attempt callbacks, execution-start observations, and cancellation acknowledgements may continue during bounded settlement.
  * Active Attempts are not automatically cancelled merely because the orchestrator process is draining.
* **Configuration Delivery**: Domain and application logic consume validated semantic configuration and policy through explicit architectural dependencies rather than directly reading raw external configuration sources (exact loaders, formats, and validation mechanics deferred to [ADR-020](00-architecture-decision-register.md)/[ADR-023](00-architecture-decision-register.md)).
* **Abstract Time Capability**: All time-dependent domain logic (timeouts, retry backoff calculation, heartbeat expiration) obtains timestamps through an abstract control-plane time port. This abstraction supports deterministic testing and prevents correctness-critical domain logic from depending directly on uncontrolled wall-clock access (concrete clock implementation deferred to [ADR-020](00-architecture-decision-register.md)).

---

## 18. Maintenance & Code Organization

### 18.1 Repository Strategy Recommendation
NexusFlow recommends a **Monorepo with explicit package boundaries** for V1:
* **Ergonomics**: Enables a solo developer or small team to coordinate control-plane and worker protocol changes atomically within single commits and pull requests.
* **Unified Quality Assurance**: Simplifies integration testing, shared formatting, and continuous integration workflows.
* **Easier Refactoring**: Allows in-process module boundaries to be refined before committing to physical service extraction.

### 18.2 Organizational vs. Architectural Separation
**The monorepo recommendation is strictly an organizational choice, not a runtime architecture invariant.**
* If the project later transitions to multiple repositories (e.g., splitting workers or client SDKs into separate repos), the runtime and deployment boundaries remain unchanged.
* Documentation may be co-located within the monorepo or stored separately without affecting system architecture.
* Any directory layouts shown in documentation are illustrative examples; exact folder naming and package structures belong to physical implementation design ([ADR-020](00-architecture-decision-register.md)).

---

## 19. Future Evolution & Service Extraction (V2+)

Because the control plane is factored into cohesive logical modules behind semantic ports, specific subsystems may be considered for extraction into independent network services in future versions **only when operational or semantic scale justifies it**:
1. **Read-Side Query & Visualization Projections**: Extracting high-volume history audit queries, non-authoritative read projections, or workflow execution visualization into an independent read-only query service. This leaves the core control plane focused exclusively on atomic state transitions while offloading read pressure. (Diagnostic log streaming remains an observability concern under [ADR-016](adr-016-observability-architecture.md)/[ADR-020](00-architecture-decision-register.md)/[ADR-027](00-architecture-decision-register.md) as applicable).
2. **Definition Authoring & Validation Tooling**: Extracting future authoring tooling, linting, validation tooling, or SDK-related definition construction capabilities into external tooling services.
3. **Auxiliary Reporting & Analytical Services**: Extracting historical reporting, cost attribution, and analytics into a background analytical data service.

*Constraints on Extraction*:
* The authoritative history write path must remain atomically bound to state transitions within the control plane; only non-authoritative read-side projections may be extracted.
* Expression evaluation is excluded as an extraction example because V1 contains no expression language.
* Logical boundaries avoid unnecessary coupling that would make future High Availability (HA) evolution harder, but [ADR-025](00-architecture-decision-register.md) must define leadership, authority, split-brain prevention, epochs/fencing, and multi-instance coordination before HA is possible.

---

## 20. Rejected Alternatives

1. **Microservices Control Plane for V1**: Rejected. V1 has no established requirement that justifies paying the additional distributed consistency and operational complexity of decomposing correctness-coupled control-plane responsibilities across independent network services. Splitting scheduling, routing, worker coordination, and history into separate services would make atomic consistency groups materially harder to preserve.
2. **Database-Per-Module Architecture**: Rejected. Core orchestration invariants require multi-entity consistency groups (e.g., attempt ownership, state+history commits) to execute atomically. Partitioning persistence stores by module would require complex distributed coordination across critical state transitions.
3. **Worker Direct-to-Persistence Architecture**: Rejected. Allowing workers to write execution outcomes directly to authoritative orchestration persistence bypasses control-plane state machines, eliminates concurrency fencing, risks credential exposure, and creates uncontained split-brain failure modes.
4. **Event-Sourced Control Plane**: Rejected. Storing only event streams and reconstructing current state via event replay conflicts with the selected NexusFlow model: normalized durable current state is authoritative ([ADR-011](adr-011-state-persistence-strategy.md)), [ADR-012](adr-012-recovery-strategy.md) recovery inspects current state, and [ADR-014](adr-014-execution-history-and-audit-model.md) history is an append-oriented audit log, not orchestration truth. Adopting event sourcing would unnecessarily redefine already-established persistence and recovery semantics.
5. **Mandatory Global Event Bus Core**: Rejected. Making an internal publish-subscribe event bus the authoritative source of orchestration progression creates risks around ephemeral event loss and introduces consistency/coordination concerns between state persistence and event publication.
6. **Per-Attempt Lease Model**: Rejected. As established in [ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md) and [ADR-008](adr-008-worker-coordination-and-liveness-model.md), attempt ownership is governed by `WorkerSession` liveness and authoritative attempt binding, not renewable leases.
7. **JSON Definition Authoring in V1**: Rejected. V1 authoring input is strictly YAML ([ADR-002](adr-002-workflow-definition-parsing-strategy.md)). Additional authoring formats may be added in future versions.

---

## 21. Decision Evolution

* **ADR-001 through ADR-018**: Established individual semantic subsystems: IWS normalization, canonical DAG representation, validation rules, scheduling conditions, state machine lifecycles, worker coordination, routing rules, data flow bindings, persistence strategies, recovery reconciliation, concurrency fencing, audit logging, public API design, observability, graceful shutdown, and error classification.
* **ADR-019 (This Record)**: Synthesizes the architectural boundaries of the entire system. It rejects premature microservice decomposition, establishes the Modular Monolith control plane, defines the single external worker runtime boundary, formalizes the state ownership matrix, and enforces the Inverted Dependency (Ports and Adapters) model.
* **ADR-020 (Technology Selection Strategy)**: Will select the concrete programming language, web framework, storage drivers, worker wire transport, and build packaging based on the boundaries formalized here.

---

## 22. Common Misconceptions

1. **"Modular monolith means all code is tangled in one package."**  
   *Correction*: A modular monolith enforces strict logical module separation, explicit in-process interfaces, and clean dependency inversion. The code is modular; only the physical deployment artifact is unified.
2. **"Workers can update task status directly in authoritative persistence to reduce network hops."**  
   *Correction*: Workers execute external task logic across a separate runtime boundary and are not trusted with authoritative orchestration mutation capability. Allowing direct persistence access violates control-plane authority, bypasses state machine validation, breaks concurrency fencing, and creates security vulnerabilities.
3. **"Every logical component must be an independent network service to scale."**  
   *Correction*: The control plane manages orchestration semantics, while compute-intensive user task execution is offloaded to horizontally scalable external worker runtimes.
4. **"The control plane has zero network dependencies."**  
   *Correction*: The only *NexusFlow-owned semantic runtime distribution boundary required in V1* is Control Plane $\longleftrightarrow$ Worker Runtime. External storage and telemetry backends may be remote infrastructure dependencies, but they are external infrastructure dependencies, not independent NexusFlow orchestration services.
5. **"Monorepo is an architectural correctness requirement for NexusFlow."**  
   *Correction*: The monorepo is an organizational recommendation for V1 developer ergonomics. The runtime architecture is completely independent of whether code is stored in one repository or several.
6. **"Execution history can be updated asynchronously in a background queue."**  
   *Correction*: [ADR-014](adr-014-execution-history-and-audit-model.md) requires the semantic state mutation and required `HistoryEntry` to participate in the same atomic persistence commit, preventing one from being authoritatively committed without the other under the selected persistence semantics.

---

## 23. Open Questions (Delegated to Future ADRs)

All primary architectural boundaries are resolved in ADR-019. The following implementation-level decisions are intentionally delegated:
* **Concrete Language, Framework, and Wire Protocol Selection**: Delegated to [ADR-020](00-architecture-decision-register.md).
* **Concrete Physical Package Layout and Project Directory Trees**: Delegated to [ADR-020](00-architecture-decision-register.md) and High-Level Design (HLD).
* **Concrete Testing Harnesses and Mocking Tooling**: Delegated to [ADR-021](00-architecture-decision-register.md).
* **Worker Protocol Authentication (mTLS, Tokens)**: Delegated to [ADR-022](00-architecture-decision-register.md).
* **Numeric Capacity Limits, Timeouts, and Buffer Capacities**: Delegated to [ADR-023](00-architecture-decision-register.md).
* **Multi-Instance Orchestrator Clustering and Distributed Consensus**: Delegated to [ADR-025](00-architecture-decision-register.md).
* **Multi-Language Worker Client SDK Architecture**: Delegated to [ADR-026](00-architecture-decision-register.md).

---

## 24. Interview Discussion & Architectural Defense

* **Why a modular monolith instead of microservices for the control plane?**  
  *Orchestration is inherently consistency-intensive. Task readiness, attempt ownership, state settlement, and audit logging require multi-entity atomic transactions. Decomposing the control plane into microservices would split consistency-coupled mutations across independently authoritative boundaries, requiring distributed coordination mechanisms that are unnecessary for V1. A modular monolith provides clean in-process modularity and strong testability while preserving local transactional atomicity.*
* **Why separate workers across a network boundary if the control plane is a monolith?**  
  *Workers execute arbitrary external user-defined code with unpredictable resource demands, potential memory leaks, unhandled runtime failures, and third-party dependency conflicts. Running task execution inside the control-plane process memory would violate failure containment. Distributing workers across separate runtime boundaries protects the control plane, allows heterogeneous execution environments, and enables independent horizontal scaling.*
* **Why not adopt a database-per-module architecture?**  
  *Database-per-module breaks atomic consistency groups. Transitions such as Attempt ownership (which updates task status, creates an attempt record, binds a worker session, and records an audit event) must succeed or fail as a single atomic unit. A single logical persistence boundary preserves this atomicity cleanly.*
* **How does the architecture prevent the modular monolith from degenerating into an unmaintainable codebase?**  
  *Through strict dependency inversion (Ports and Adapters) and an explicit Application use-case layer. Domain entities have zero dependencies on external transport frameworks or storage drivers. Ingress interfaces cannot invoke persistence directly. All state mutations are coordinated by application use cases using semantic port interfaces. Explicit dependency rules can later be reinforced through package/module structure, static analysis, linting, or architectural tests depending on the selected technology.*
* **Why is execution history not an independent asynchronous service?**  
  *In an orchestration engine, the audit log must reflect committed reality exactly. [ADR-014](adr-014-execution-history-and-audit-model.md) requires the semantic state mutation and required HistoryEntry to participate in the same atomic persistence commit, preventing state and audit history from diverging due to uncoordinated or failing background flushes.*

---

## 25. References

* [ADR-001: Internal Workflow Specification (IWS)](adr-001-internal-workflow-specification.md)
* [ADR-002: Workflow Definition Parsing Strategy](adr-002-workflow-definition-parsing-strategy.md)
* [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md)
* [ADR-004: Workflow Validation Strategy](adr-004-workflow-validation-strategy.md)
* [ADR-005: Workflow Task Scheduling & Dispatch Architecture](adr-005-workflow-task-scheduling-and-dispatch-architecture.md)
* [ADR-006: Workflow Execution State Machine](adr-006-workflow-execution-state-machine.md)
* [ADR-007: Task Execution Lifecycle & Attempt Model](adr-007-task-execution-lifecycle-and-attempt-model.md)
* [ADR-008: Worker Coordination & Liveness Model](adr-008-worker-coordination-and-liveness-model.md)
* [ADR-009: Task Routing Strategy](adr-009-task-routing-strategy.md)
* [ADR-010: Workflow Data Flow & Parameter Passing](adr-010-workflow-data-flow-and-parameter-passing.md)
* [ADR-011: State Persistence Strategy](adr-011-state-persistence-strategy.md)
* [ADR-012: Recovery Strategy](adr-012-recovery-strategy.md)
* [ADR-013: Consistency & Concurrency Strategy](adr-013-consistency-and-concurrency-strategy.md)
* [ADR-014: Execution History & Audit Model](adr-014-execution-history-and-audit-model.md)
* [ADR-015: External API Architecture](adr-015-external-api-architecture.md)
* [ADR-016: Observability Architecture](adr-016-observability-architecture.md)
* [ADR-017: Graceful Shutdown Architecture](adr-017-graceful-shutdown-architecture.md)
* [ADR-018: Error Handling Philosophy](adr-018-error-handling-philosophy.md)
* [Architecture Decision Register](00-architecture-decision-register.md)

---

## 26. Traceability Matrix

| Requirement / Invariant | Governing ADRs | ADR-019 Architectural Manifestation | Downstream Realization |
| :--- | :--- | :--- | :--- |
| **Control-Plane Modularity** | FR-BND-001, FR-BND-004 | Single deployable control-plane unit containing 8 cohesive logical responsibilities. | [ADR-020](00-architecture-decision-register.md) packaging; HLD. |
| **Worker Blast Radius Isolation** | FR-BND-002, NFR-BND-002 | Worker runtime independently deployable; executes in separated worker-local runtime environment. | [ADR-020](00-architecture-decision-register.md), Worker LLD. |
| **Authoritative State Integrity** | FR-BND-003, ADR-006, ADR-007 | Control plane sole orchestration authority; workers report observations only; no direct worker persistence mutation. | Application Use Cases, Security [ADR-022](00-architecture-decision-register.md). |
| **Atomic Multi-Entity Commits** | FR-BND-005, ADR-011, ADR-013 | Single logical persistence boundary supporting atomic consistency groups without distributed 2PC. | Storage Adapters ([ADR-020](00-architecture-decision-register.md)). |
| **Inward Dependency Direction** | FR-BND-006, NFR-BND-003 | Ports and Adapters architecture; core domain independent of storage/transport/frameworks. | Codebase layout; unit test suites. |
| **Exact Task Routing** | ADR-009 | Evaluates RUNNABLE task, live WorkerSession, `accepting_new_work = true`, exact Activity Type. | Routing component, dispatch use case. |
| **Worker Liveness & Fencing** | ADR-008, ADR-013 | Authoritative attempt ownership tied to `WorkerSession`; OCC concurrency fencing; no leases. | Concurrency guards, callback handler. |
| **State + History Atomicity** | ADR-011, ADR-014 | State transitions and `HistoryEntry` commit within the same atomic persistence boundary. | Persistence Port implementations. |
| **Current-State Recovery** | ADR-012, ADR-013 | Startup and runtime reconciliation inspects current authoritative state; no checkpoint scans. | Recovery subsystem. |
| **Non-Authoritative Telemetry** | ADR-016 | Telemetry is cross-cutting, best-effort, and fail-open. | Observability Adapters. |
| **Developer Ergonomics** | NFR-BND-001 | Monorepo recommended for V1; distinct from runtime architecture boundaries. | Repo setup, CI workflows. |

---

## 27. Decision Validation Checklist

- [x] **Core Architecture Preserved**: Modular Monolith Control Plane + External Distributed Worker Runtime.
- [x] **Distributed Boundary Exactness**: Only NexusFlow-owned semantic runtime distributed boundary is Control Plane $\longleftrightarrow$ Worker Runtime.
- [x] **Terminology Formalized**: Explicitly distinguishes Module, Deployable Process, Network Service, and Repository.
- [x] **Deployment Wording**: Uses "single deployable control-plane unit/process/artifact in V1" (no "single binary/executable" mandate).
- [x] **8 Logical Responsibilities**: Documented as cohesive logical responsibilities, not frozen physical package counts or class names.
- [x] **Conceptual Application Layer**: Explicitly documents application use cases coordinating domain logic and persistence ports.
- [x] **Interface Invariant**: Public API and Worker Protocol handlers cannot bypass the application layer to mutate persistence directly.
- [x] **Dependency Direction**: Inverted dependency model (Ports and Adapters) documented; domain code has zero external dependencies.
- [x] **Definition Truth Model**: Validated IWS is durable truth; Canonical Graph is reconstructible; AST metadata is diagnostic (not mandatory durable).
- [x] **Data Flow Exactness**: Task input is stable across retries; task output belongs to successful outcome; workflow output exists only in `SUCCEEDED`.
- [x] **Scheduler Boundary**: Operates on execution state; no separate durable state; no concurrency caps or mandatory sweeps in V1.
- [x] **Routing Boundary**: Exact canonical Activity Type match only; no queues, tags, affinity, or resource constraints.
- [x] **Worker Coordination Boundary**: Ephemeral worker registry; authoritative attempt ownership bound to `WorkerSession`; no lease model.
- [x] **Worker Execution Boundary**: Executes in worker-local runtime environment; no mandatory thread/subprocess/container mandate.
- [x] **Worker Protocol Boundary**: Explicit versionable protocol contract; transport-neutral in ADR-019 (no HTTP/REST/gRPC frozen).
- [x] **Persistence Boundary**: One logical authoritative persistence boundary supporting atomic consistency groups; no SQL/table schemas frozen.
- [x] **History Integrity**: Append-oriented audit log committed atomically with state; not an event source; not an async service in V1.
- [x] **Recovery Integrity**: Startup and runtime reconciliation inspects current state; no checkpoint cursors or event replays.
- [x] **Telemetry Integrity**: Cross-cutting, best-effort, fail-open; no specific vendor tooling frozen.
- [x] **Repository Strategy**: Monorepo recommended for V1 ergonomics; explicitly decoupled from runtime architecture invariants.
- [x] **Future Extraction**: Defensible criteria established (read projections, definition tooling); no expression worker fiction.
- [x] **Technology Neutrality**: Concrete languages, frameworks, drivers, and wire protocols deferred to ADR-020.
- [x] **Status & Structure**: Approved — Not Frozen; complete 27-section structure fully satisfied.
