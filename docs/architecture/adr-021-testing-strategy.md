# ADR-021: Testing Strategy

## 1. Purpose

This Architecture Decision Record (ADR) defines the testing architecture, verification philosophy, and quality assurance framework for NexusFlow V1. It establishes how NexusFlow systematically verifies its orchestration semantics, concurrency control, transactional integrity, worker coordination, failure recovery, and external contracts while remaining practical for a solo engineer and suitable for fast, reliable continuous integration (CI).

---

## 2. Context

NexusFlow V1 is an asynchronous workflow orchestrator built using Python 3.12, `asyncio`, FastAPI, Uvicorn, Pydantic v2, PostgreSQL 16, SQLAlchemy 2.0 Async, `asyncpg`, and Alembic (as decided in [ADR-020](adr-020-technology-selection.md)). Orchestration truth is maintained in PostgreSQL under `READ COMMITTED` isolation augmented by explicit integer-revision Optimistic Concurrency Control (OCC) ([ADR-013](adr-013-consistency-and-concurrency.md)). Coordination with distributed Python V1 workers occurs over an HTTP/JSON pull/long-poll protocol governed by a two-phase Candidate $\to$ Ownership handshake ([ADR-008](adr-008-worker-coordination-and-liveness.md)).

Distributed asynchronous orchestration systems present complex failure modes. Bugs in these systems rarely present as simple single-threaded logic errors; rather, they emerge from:
- Multi-connection OCC races where two workers claim the same task simultaneously,
- Stale or duplicate callbacks arriving after an attempt timeout or workflow cancellation,
- Partial persistence failures interrupting multi-entity consistency group commits,
- Process crashes occurring while an execution is partially initialized,
- Divergence between ephemeral in-memory worker registry state and durable PostgreSQL records,
- Recovery routines that could re-execute completed tasks or introduce duplicate progression.

Verifying such an engine cannot rely on superficial endpoint tests or blind code coverage metrics. NexusFlow requires an explicit verification architecture designed to bridge high-level architectural invariants to executable, reproducible test harnesses.

---

## 3. Problem Statement

How should NexusFlow V1 verify that its orchestration semantics remain correct under:
- Normal execution,
- Concurrency and race conditions,
- Duplicate operations and idempotent retries,
- Stale observations and late callbacks,
- Process crashes and restart recovery,
- Ambiguous or unknown persistence commit outcomes,
- Worker disconnects and liveness failures,
- Timeout and cancellation races,
- Injected infrastructure faults,
- Malformed inputs and protocol deviations,
- Telemetry outages and transport failures,

while keeping the test suite fast, deterministic, maintainable by a solo developer, and free of flaky timing-dependent assertions?

---

## 4. Requirements Covered

- **REQ-TEST-001 (Invariant-Driven Verification)**: The test architecture must verify observable domain semantics and architectural invariants rather than coupling to internal implementation details.
- **REQ-TEST-002 (Adversarial Race Verification)**: Concurrency control, OCC revisions, and state-machine transitions must be verified under explicit race interleavings using real database transactions.
- **REQ-TEST-003 (Deterministic Timing)**: Time-dependent behavior (deadlines, timeouts, backoff delays) must be verified deterministically without wall-clock sleeps.
- **REQ-TEST-004 (Atomicity & Consistency Groups)**: Transactional boundaries defined in [ADR-011](adr-011-state-persistence.md) (state + output + history) must be verified for all-or-nothing atomicity.
- **REQ-TEST-005 (Crash Recovery & Idempotency)**: Crash recovery routines defined in [ADR-012](adr-012-recovery.md) must be verified across all valid lifecycle states and confirmed to be strictly idempotent.
- **REQ-TEST-006 (Contract Integrity)**: Worker protocol ([ADR-008](adr-008-worker-coordination-and-liveness.md)) and public REST API ([ADR-015](adr-015-external-api-architecture.md)) must be verified at the wire boundary for schema compliance and error mapping ([ADR-018](adr-018-error-handling-philosophy.md)).
- **REQ-TEST-007 (Fast & Layered CI)**: The test portfolio must be organized into logical layers so that developers receive immediate feedback from in-memory tests while heavier infrastructure tests run reliably in CI.

---

## 5. Constraints

- **Python 3.12 & asyncio**: All test suites, harnesses, and drivers must operate natively within Python 3.12 and the `asyncio` event loop model.
- **PostgreSQL 16 Semantics**: Concurrency and persistence tests must run against real PostgreSQL 16; in-memory relational emulators (e.g., SQLite) cannot be used for concurrency or transaction testing because they lack PostgreSQL's row locking, `READ COMMITTED` snapshot semantics, and native JSONB operators.
- **No Production Code Pollution**: Domain models, state machines, and application use cases must not contain test-only boolean flags, synthetic branches, or mock hooks.
- **Solo Developer Ergonomics**: Test harnesses must be runnable locally without complex external orchestration (e.g., no mandatory Kubernetes cluster or external cloud dependencies).
- **Zero Wall-Clock Sleeps in Unit/Integration Layers**: Flaky time-based delays are prohibited; deterministic synchronization and logical clocks must govern timing.

---

## 6. Goals

- Define a comprehensive, layered testing portfolio from pure domain logic up to multi-container end-to-end smoke scenarios.
- Provide direct traceability from architectural invariants established in ADR-001 through ADR-020 to executable test suites.
- Establish deterministic testing techniques for OCC conflicts, multi-connection race windows, and ambiguous commit outcomes.
- Specify fault-injection points at port and adapter boundaries to verify resilience without compromising production domain code.
- Define a clear CI tiering model that separates blocking pull-request gates from heavier post-merge or release verification.

---

## 7. Non-Goals

- **No Vanity Coverage Targets**: ADR-021 does not mandate arbitrary numerical coverage targets (e.g., 85% or 95%); coverage metrics serve as regression guardrails while invariant completeness remains the primary quality oracle.
- **No Mandatory Multi-Version Matrix**: Testing multiple Python or PostgreSQL versions is excluded for V1; only Python 3.12 and PostgreSQL 16 are verified.
- **No Performance Threshold Guarantees in V1**: Formal performance benchmarking and load testing (via `k6`) are non-blocking and deferred until empirical baselines exist.
- **No Kubernetes Chaos Testing**: Heavyweight chaos platforms (e.g., Chaos Mesh, Gremlin) are rejected for V1 in favor of adapter-level fault injection.
- **No Security Mechanism Selection**: ADR-021 defines testing boundaries for security controls, but does not select authentication or authorization mechanisms (reserved for [ADR-022](adr-022-security-model.md)).

---

## 8. Candidate Solutions

### Candidate A: Conventional Unit and Endpoint Integration Testing
- Focuses on unit tests with extensive mocking, supplemented by standard HTTP endpoint tests against a test database.
- **Evaluation**: Fails to systematically expose multi-connection concurrency races, OCC conflicts, distributed worker session fencing, or crash recovery edge cases. Relies heavily on happy-path assertions.

### Candidate B: Heavy End-to-End (E2E) Centric Testing
- Relies primarily on black-box containerized system testing driving the entire engine through public endpoints and simulated worker containers.
- **Evaluation**: Computationally expensive, slow to execute, prone to non-deterministic timing flakiness, and provides poor diagnostic localization when subtle race windows fail.

### Candidate C: Layered Invariant-Driven Testing (Selected)
- Implements a multi-layered verification strategy where each layer owns distinct responsibilities:
  1. Pure domain/unit tests verify algorithms, DAGs, and state transitions in memory.
  2. Property-based and stateful model tests (via Hypothesis) discover edge cases and sequence invariants.
  3. Application use-case tests verify orchestration flows through semantic ports using test doubles.
  4. Real PostgreSQL 16 integration tests verify transactions, constraints, JSONB mapping, and Alembic migrations.
  5. Deterministic concurrency harnesses verify multi-connection OCC races using explicit barriers.
  6. Worker protocol and API contract tests verify wire-level communication and error mapping.
  7. Fault-injection adapters verify handling of unknown commits, timeouts, and network drops.
  8. Recovery suites verify idempotent crash recovery from persisted database snapshots.
  9. A focused Docker Compose E2E smoke suite verifies baseline integrated multi-process execution.
  10. Static quality gates enforce type safety, linting, and architectural boundaries.

### Candidate D: Formal Verification Heavy Strategy (TLA+ / Alloy)
- Develops and maintains formal mathematical specifications alongside the Python codebase.
- **Evaluation**: Prohibitive overhead and maintenance burden for a solo developer in V1. High risk of specification divergence from executable Python code without eliminating the need for implementation testing.

---

## 9. Detailed Evaluation

| Dimension | Candidate A (Unit + Endpoint) | Candidate B (E2E Heavy) | Candidate C (Layered Invariant-Driven) | Candidate D (Formal Verification) |
| :--- | :--- | :--- | :--- | :--- |
| **Race & OCC Verification** | Poor (mocks obscure races) | Poor (races difficult to force) | **Superior (deterministic barriers)** | High (model level only) |
| **Diagnostic Localization** | High for simple bugs | Very Poor (log inspection) | **High (isolated test layers)** | Poor (abstract model only) |
| **Execution Speed in CI** | Fast | Slow | **Balanced (fast gates, thorough suites)**| N/A |
| **Maintenance Burden** | Moderate | High (flaky E2E maintenance) | **Sustainable for Solo Developer** | Very High (dual modeling) |
| **Fidelity to PostgreSQL** | Low (often relies on SQLite) | High | **High (real PostgreSQL 16 mandatory)**| None |
| **Crash Recovery Coverage** | Poor | Coarse | **Exhaustive (snapshot-based suites)** | Abstract |

---

## 10. Decision

NexusFlow V1 adopts a **Layered Invariant-Driven Testing Strategy**. Correctness is verified through complementary, decoupled layers designed to enforce architectural invariants and observable semantics rather than implementation details:

1. **Pure Domain / Unit Testing**: Fast, in-memory validation of DAG validation, topological sorting, state transitions, data binding expressions, and error classifications without external infrastructure.
2. **Table-Driven State-Machine Testing**: Exhaustive transition matrices verifying permitted transitions, forbidden transitions, and terminal immutability for `WorkflowExecution`, `TaskExecution`, and `ExecutionAttempt`.
3. **Property-Based Testing**: Hypothesis generators verifying graph properties (acyclicity, edge preservation, topological ordering) and data flow resolution across arbitrary JSON structures.
4. **Stateful Model-Based Testing**: Hypothesis `RuleBasedStateMachine` verifying execution invariants against randomized command sequences.
5. **Application Use-Case Testing**: Port-and-adapter execution of application use cases using test doubles to verify coordination, transactional consistency group boundaries, and error propagation.
6. **Real PostgreSQL 16 Integration Testing**: Persistence verification against real PostgreSQL 16 covering `READ COMMITTED` transactions, Alembic migrations, foreign keys, JSONB mapping, and database constraints.
7. **Deterministic OCC / Concurrency Testing**: Multi-connection race testing using explicit synchronization barriers (`asyncio.Barrier`) to verify single-winner semantics, stale-write rejections, and fencing.
8. **Worker Protocol Contract Testing**: Wire-level verification of the HTTP/JSON worker protocol covering registration, capability matching, Candidate $\to$ Ownership two-phase coordination, start observations, callbacks, and malformed payload rejection.
9. **Public API Contract Testing**: External REST API verification covering idempotency key semantics, cursor pagination, filtering, and uniform machine-readable error envelopes per [ADR-015](adr-015-external-api-architecture.md) and [ADR-018](adr-018-error-handling-philosophy.md).
10. **Failure-Injection Testing**: Adapter-level decorators simulating storage drops, ambiguous/unknown commit outcomes, worker transport failures, and telemetry exporter outages.
11. **Recovery and Reconciliation Testing**: Verification of startup reconciliation, lost-wakeup rediscovery, expired deadline settlement, and recovery idempotency strictly from PostgreSQL snapshots without history replay.
12. **Focused Docker Compose E2E Testing**: Containerized smoke suite verifying baseline end-to-end execution scenarios across control plane, PostgreSQL, and distributed Python workers.
13. **Static Architecture and Quality Checks**: Static analysis via Ruff, Pyright, and explicit pytest import-boundary tests enforcing modular monolith separation.

---

## 11. Decision Rationale

### 11.1 Principle: Invariants Over Implementation Details
Orchestrator correctness is defined by state invariants:
- A terminal state is immutable.
- A downstream task is never `RUNNABLE` before all direct dependencies are terminal `SUCCEEDED`.
- A task execution never has two active authoritative ownerships.
- Authoritative state progression and history audit entries commit atomically.
- An execution attempt cannot skip from `CLAIMED` to `SUCCEEDED` without an authoritative start observation.
- Stale or duplicate callbacks cannot corrupt newer attempts or terminal tasks.

Testing these invariants directly ensures the system remains correct across refactorings, whereas tests asserting specific internal method calls or SQL text become fragile and provide false confidence.

### 11.2 Principle: Adversarial Concurrency and Determinism
Distributed bugs occur in edge-case interleavings. NexusFlow uses explicit synchronization barriers (`asyncio.Barrier`) within test persistence adapters. By pausing concurrent coroutines after reading revision $N$ but prior to executing conditional updates, tests reproduce race conditions deterministically on real PostgreSQL transactions without flaky timing sleeps.

### 11.3 Principle: Real PostgreSQL for True Concurrency
SQLite and in-memory mocks do not implement PostgreSQL's multi-version concurrency control (MVCC), `READ COMMITTED` isolation, row-level locks, partial indexes, or native JSONB dialect. Relying on SQLite for concurrency testing would conceal the very failure modes NexusFlow is engineered to prevent.

---

## 12. Tradeoffs

- **Harness Complexity vs. Test Determinism**: Implementing synchronization barriers and adapter fault decorators requires deliberate harness design, but eliminates flaky timing bugs and produces reliable CI runs.
- **PostgreSQL Dependency vs. Execution Speed**: Running integration tests against real PostgreSQL 16 requires database provisioning (via `testcontainers-python` locally or GitHub Actions service containers in CI), but guarantees absolute fidelity to production transactional semantics.
- **Layered Redundancy vs. Diagnostic Precision**: Multiple layers test overlapping concerns (e.g., state machines are tested in pure domain, property, and integration layers). This redundancy is deliberate: domain tests localize logic bugs in milliseconds, while integration tests verify transactional execution against PostgreSQL.

---

## 13. Consequences

### 13.1 Positive Consequences
- **High Diagnostic Localization**: Failures are immediately isolated to domain logic, transactional persistence, wire protocol, or system integration depending on which layer fails.
- **Flake-Free CI**: Eliminating wall-clock sleeps in favor of a fake `Clock` port and synchronization barriers ensures deterministic test runs.
- **Refactoring Safety**: Tests asserting observable domain semantics and persisted contracts remain stable during internal architectural refactorings.
- **Auditable Quality**: Critical invariants trace directly back to governing ADRs.

### 13.2 Negative Consequences & Liabilities
- **Test Infrastructure Overhead**: Developers must have Docker available locally to run PostgreSQL integration tests via Testcontainers.
- **Dual-Model Maintenance for Stateful Tests**: Maintaining Hypothesis `RuleBasedStateMachine` requires keeping the simplified reference model aligned with domain rules.

---

## 14. Failure Modes

### 14.1 Test Flakiness Due to Timeouts
- *Cause*: Inappropriate use of real wall-clock sleeps or uncoordinated concurrent tasks.
- *Mitigation*: Zero wall-clock sleeps permitted; all time advancement must use the fake `Clock` port. Concurrency must use `asyncio.Barrier`.

### 14.2 False Confidence from In-Memory Substitutes
- *Cause*: Attempting to run persistence or concurrency tests against SQLite or mock sessions.
- *Mitigation*: Architectural rule strictly prohibits non-PostgreSQL databases for persistence and concurrency suites.

### 14.3 State Leakage Across Database Tests
- *Cause*: Tests modifying shared tables without clean teardown or isolation.
- *Mitigation*: Integration tests run within rolled-back transactions or use table truncation; concurrency tests execute against dedicated isolated schemas.

---

## 15. Debugging

When a test failure occurs, harnesses must output sufficient diagnostics to reproduce the failure locally:
- **Property Test Failures**: Hypothesis automatically prints the minimal falsifying example and the exact random seed needed to reproduce the failure.
- **Concurrency Test Failures**: Harness logs output the entity IDs, expected revisions, actual database state at failure, and the interleaved transaction commit order.
- **E2E Container Failures**: CI retains container logs (`docker compose logs`) and database error logs as downloadable build artifacts.

---

## 16. Testing

ADR-021 itself governs testing architecture. The test suite structure is organized as follows:

```
tests/
├── domain/                  # Pure in-memory unit tests (no DB, no HTTP)
│   ├── test_graph.py        # Graph algorithms, topological sort, cycle detection
│   ├── test_validation.py   # IWS semantic validation
│   ├── test_state_machine.py# Table-driven state transition matrices
│   └── test_data_flow.py    # Literal, WorkflowInput, TaskOutput bindings
├── property/                # Hypothesis property-based and stateful tests
│   ├── test_dag_properties.py
│   └── test_stateful_execution.py # RuleBasedStateMachine
├── application/             # Use-case tests with port fakes/test doubles
│   ├── test_start_execution.py
│   ├── test_claim_ownership.py
│   └── test_reconciliation.py
├── integration/             # Real PostgreSQL 16 integration tests
│   ├── test_migrations.py   # Alembic migrations & drift detection
│   ├── test_persistence.py  # JSONB mapping, constraints, consistency groups
│   └── test_concurrency.py  # Deterministic OCC barrier races
├── contract/                # Wire-level protocol and API tests
│   ├── test_worker_protocol.py # HTTP/JSON candidate/ownership/callback contract
│   └── test_public_api.py      # REST API, idempotency, cursor pagination
├── recovery/                # Crash recovery and reconciliation tests
│   ├── test_crash_recovery.py
│   └── test_lost_wakeup.py
├── e2e/                     # Focused Docker Compose multi-process smoke suite
│   └── test_e2e_scenarios.py
└── architecture/            # Static boundary tests
    └── test_import_boundaries.py
```

---

## 17. Operational Considerations

- **Local Developer Workflow**: Developers can run `pytest tests/domain tests/property tests/application` in seconds without running Docker. Running the complete integration suite uses `testcontainers-python` transparently when Docker is running.
- **CI Resource Consumption**: The blocking PR suite runs inside standard GitHub Actions runners using a native PostgreSQL service container, completing rapidly without external dependencies.
- **Extended Test Execution**: Heavier suites (Docker Compose E2E, extended property exploration, soak runs) execute post-merge, on schedule, or before releases.

---

## 18. Maintenance

- **Adding New State Transitions**: Whenever lifecycle states or transitions evolve, update the table-driven transition matrices in `tests/domain/test_state_machine.py` and the reference model in `tests/property/test_stateful_execution.py`.
- **Schema Changes**: Any SQLAlchemy model change must be accompanied by an Alembic migration script; the automated CI migration drift check verifies this synchronization.
- **Defect Reproduction**: Any discovered correctness defect must receive an automated regression test reproducing the failure prior to merging the fix.

---

## 19. Future Evolution

- **ADR-022 (Security Model)**: Once ADR-022 selects authentication and authorization mechanisms (e.g., mTLS, API tokens), dedicated contract and negative authorization tests will be incorporated into the contract layer.
- **ADR-023 (Configuration)**: Boundary limit and configuration validation tests will be added once exact configuration parameters and defaults are established.
- **ADR-025 (High Availability)**: Multi-node control plane validation will introduce distributed leader/fencing tests and multi-instance race harnesses.
- **Load and Performance Testing**: `k6` test scripts will be developed to benchmark workflow creation, claim throughput, and long-poll efficiency once functional stability is proven.

---

## 20. Rejected Alternatives

- **Rejecting SQLite for Integration/Concurrency Testing**: SQLite lacks PostgreSQL’s locking semantics, `READ COMMITTED` MVCC, partial indexing, and native JSONB dialect. Using it introduces severe risk of false-positive concurrency test results.
- **Rejecting Sleep-Based Race Testing**: Using `asyncio.sleep()` to simulate races is non-deterministic and creates flaky tests that fail under varying CPU load.
- **Rejecting Arbitrary Numeric Coverage Floors (e.g., 85% / 95%)**: Arbitrary coverage metrics incentivize writing trivial tests for non-critical code while failing to ensure distributed invariants are verified.
- **Rejecting External Chaos Engineering Frameworks for V1**: Tools like Chaos Mesh add significant operational complexity without providing finer control than adapter-level fault injection.
- **Rejecting Tox / Nox**: With Python 3.12 fixed as the single supported runtime and `uv` managing environments, extra multi-environment orchestration tools add unnecessary friction.

---

## 21. Decision Evolution

- **Initial Concept**: Standard unit tests supplemented by black-box API integration tests.
- **Architectural Calibration**: Recognized that asynchronous orchestration invariants (OCC conflicts, worker fencing, recovery idempotency, atomicity) require explicit, layered testing using real PostgreSQL transactions, synchronization barriers, and property-based stateful modeling.
- **Boundary Precision**: Refined to ensure no production domain code is polluted with test hooks, arbitrary coverage percentages are replaced by invariant verification, and timing logic is controlled deterministically via a fake `Clock` port.

---

## 22. Common Misconceptions

- *Misconception*: "High code coverage means the orchestrator is correct."  
  *Correction*: A test suite can achieve 100% statement coverage on happy-path code while completely failing to detect multi-connection OCC race conditions, stale callback overwrites, or crash recovery bugs.
- *Misconception*: "We can use SQLite in memory to run database tests faster."  
  *Correction*: SQLite does not reproduce PostgreSQL's concurrency behavior or JSONB semantics. Database integration tests must run against real PostgreSQL 16.
- *Misconception*: "Tests need to simulate crashes by physically killing the OS process in every test."  
  *Correction*: Crash recovery operates on PostgreSQL snapshots. Testing snapshot recovery in integration tests validates the exact same state restoration logic without the overhead of physical OS process termination.

---

## 23. Open Questions

- *None*. All testing tools, layer boundaries, and semantic invariants align fully with ADR-001 through ADR-020.

---

## 24. Interview Discussion

In systems engineering interviews, NexusFlow's testing architecture illustrates how to verify complex distributed semantics reliably:
- Rather than relying on fragile black-box integration tests or superficial code coverage metrics, the engine validates correctness through layered invariant-driven verification.
- Concurrency races and OCC conflicts are tested deterministically using synchronization barriers (`asyncio.Barrier`) across real PostgreSQL transactions, completely avoiding flaky sleep-based timing hacks.
- Time-based lifecycles (timeouts, deadlines, retries) are driven through a deterministic fake `Clock` port, enabling instantaneous verification of deadline sweeps and race conditions.

---

## 25. References

- [ADR-001: Internal Workflow Specification (IWS)](adr-001-internal-workflow-specification.md)
- [ADR-003: Canonical Workflow Graph](adr-003-canonical-workflow-graph.md)
- [ADR-004: Semantic Validation](adr-004-semantic-validation.md)
- [ADR-005: Task Scheduling & Eligibility](adr-005-task-scheduling-and-eligibility.md)
- [ADR-006: WorkflowExecution State Machine](adr-006-workflow-execution-state-machine.md)
- [ADR-007: TaskExecution Lifecycle & ExecutionAttempt Model](adr-007-task-execution-lifecycle-and-execution-attempt-model.md)
- [ADR-008: Worker Coordination & Liveness](adr-008-worker-coordination-and-liveness.md)
- [ADR-009: Task Routing](adr-009-task-routing.md)
- [ADR-010: Workflow Data Flow](adr-010-workflow-data-flow.md)
- [ADR-011: State Persistence](adr-011-state-persistence.md)
- [ADR-012: Recovery](adr-012-recovery.md)
- [ADR-013: Consistency & Concurrency](adr-013-consistency-and-concurrency.md)
- [ADR-014: Execution History & Audit](adr-014-execution-history-and-audit.md)
- [ADR-015: External API Architecture](adr-015-external-api-architecture.md)
- [ADR-016: Observability](adr-016-observability.md)
- [ADR-017: Graceful Shutdown](adr-017-graceful-shutdown.md)
- [ADR-018: Error Handling Philosophy](adr-018-error-handling-philosophy.md)
- [ADR-019: Project & Service Boundaries](adr-019-project-and-service-boundaries.md)
- [ADR-020: Technology Selection](adr-020-technology-selection.md)
- Hypothesis Documentation: https://hypothesis.readthedocs.io/
- Testcontainers Python: https://testcontainers-python.readthedocs.io/

---

## 26. Traceability

### 26.1 Architectural Requirement Traceability

| Requirement | Architectural Invariant | Verification Layer | Harness Mechanism |
| :--- | :--- | :--- | :--- |
| **ADR-003 / 004** | Graph is acyclic; $\mathcal{O}(V+E)$ sparse adjacency structure; cycle detection complete | Pure Domain & Property | `pytest` + `Hypothesis` DAG generators |
| **ADR-006 / 007** | State transitions valid; terminal immutability; attempt ordinals monotonic | Pure Domain & Stateful | Table-driven transition matrix + `RuleBasedStateMachine` |
| **ADR-008** | Two-phase Candidate $\to$ Ownership; session authority fencing | Contract & Concurrency | HTTP contract suite + concurrent claim harness |
| **ADR-010** | Immutable inputs; valid bindings; explicit JSON `null` distinct from missing | Pure Domain & Persistence | Data binding unit tests + JSONB persistence tests |
| **ADR-011** | Consistency groups commit all-or-nothing (state + output + history) | PostgreSQL Integration | Transaction fault injection via adapter decorator |
| **ADR-012** | Snapshot recovery is idempotent; no history replay; no YAML reparse | Recovery Suite | Repeated recovery execution against static DB snapshot |
| **ADR-013** | OCC revision mismatch aborts stale writes; single winner under race | Deterministic Concurrency| `asyncio.Barrier` synchronized multi-connection commits |
| **ADR-014** | Audit history written atomically with state; no orphan history | PostgreSQL Integration | Rollback & OCC conflict assertion against history table |
| **ADR-015 / 018**| Idempotent starts; cursor pagination; uniform error envelope; no leaked SQL | Contract / Public API | REST API test suite driving FastAPI test client |
| **ADR-016** | Telemetry failures do not block state progression; low-cardinality labels | Integration | Metric registry inspection + broken exporter fault proxy |
| **ADR-017** | `DRAINING` stops admission of new mutations; in-flight settlements finalize | Integration | Graceful shutdown test harness with mock drain window |

---

## 27. Decision Validation Checklist

- [x] **Layered Architecture Defined**: Pure domain, property, use-case, PostgreSQL integration, concurrency, contract, recovery, and E2E smoke layers explicitly specified.
- [x] **Real PostgreSQL 16 Mandatory**: In-memory SQLite strictly prohibited for persistence and concurrency testing.
- [x] **Deterministic Concurrency Specified**: `asyncio.Barrier` and adapter-level coordination hooks selected over sleep-based timing.
- [x] **Hypothesis Integration**: Selected for property-based DAG testing and `RuleBasedStateMachine` stateful modeling.
- [x] **Deterministic Time Control**: Fake `Clock` port specified for all timeout, deadline, and retry testing.
- [x] **Transactional Atomicity Verified**: Consistency groups (state + output + history) verified via adapter fault injection.
- [x] **Crash Recovery Idempotency Enforced**: Verification from PostgreSQL snapshots without history replay or YAML reparsing.
- [x] **Wire Protocol Contract Testing Defined**: Two-phase Candidate $\to$ Ownership handshake and callback fencing verified at HTTP/JSON boundary.
- [x] **No Arbitrary Coverage Targets**: Coverage defined as a regression guardrail; invariant completeness is the primary oracle.
- [x] **Static Quality Gates Established**: Ruff, Pyright, and explicit pytest import-boundary tests enforce architectural separation.
- [x] **Solo Developer & CI Feasibility**: Blocking PR suite runs rapidly with native PostgreSQL service; heavier suites run post-merge or pre-release.
- [x] **Fidelity to ADR-001 through ADR-020**: Fully preserves all state machines, coordination models, OCC revisions, and technology selections without introducing unapproved states or mechanisms.
