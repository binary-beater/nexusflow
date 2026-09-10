# ADR-023: Configuration Architecture

## 1. Purpose

This Architecture Decision Record (ADR) defines the configuration architecture, representation, validation rules, source precedence, lifecycle mutability, and operational parameterization for NexusFlow V1. It establishes how NexusFlow configures its control-plane services, distributed worker runtimes, database connectivity, security credentials, operational deadlines, and observability pipelines without compromising previously approved orchestration, scheduling, lifecycle, persistence, or recovery invariants.

---

## 2. Context

NexusFlow V1 is designed as a single-instance modular monolith control plane coordinating with distributed Python V1 workers over an HTTP/JSON pull/long-poll protocol ([ADR-019](docs/architecture/adr-019-project-and-service-boundaries.md), [ADR-020](docs/architecture/adr-020-technology-selection.md)). Orchestration correctness relies upon explicit lifecycle state machines ([ADR-006](docs/architecture/adr-006-workflow-execution-state-machine.md), [ADR-007](docs/architecture/adr-007-task-execution-lifecycle-and-execution-attempt-model.md)), two-phase Candidate $\to$ Ownership coordination ([ADR-008](docs/architecture/adr-008-worker-coordination-and-liveness.md)), transactional consistency groups ([ADR-011](docs/architecture/adr-011-state-persistence.md)), snapshot-based crash recovery ([ADR-012](docs/architecture/adr-012-recovery.md)), Optimistic Concurrency Control (OCC) guarded writes ([ADR-013](docs/architecture/adr-013-consistency-and-concurrency.md)), layered invariant testing ([ADR-021](docs/architecture/adr-021-testing-strategy.md)), and a pragmatic defense-in-depth security model ([ADR-022](docs/architecture/adr-022-security-architecture.md)).

As the final V1 platform ADR, ADR-023 bridges the abstract architectural specifications into executable operational realities. A workflow engine operates under varying operational constraints (e.g., local developer environments, resource-constrained CI runners, high-throughput Docker Compose deployments). However, in distributed orchestration systems, improper configuration management introduces severe architectural risks:
- Operational parameters may be conflated with semantic execution contracts, causing in-flight workflows to behave unpredictably across control-plane restarts.
- Configuration settings might inadvertently create "knobs" that attempt to disable foundational correctness invariants (e.g., bypassing OCC, disabling history audit logs, or relaxing task dependency satisfaction).
- Insecure default credentials or unvalidated configurations could expose control-plane APIs or leak database passwords into operational logs.
- Multi-process ASGI worker spawning could inadvertently violate the single-process invariant required by the in-process ephemeral Worker Registry.

ADR-023 establishes the boundaries of what is configurable, how configuration is validated and frozen at startup, and how semantic durability is preserved across process lifecycles.

---

## 3. Problem Statement

How should NexusFlow V1 manage configuration across:
- Control-plane server binding and HTTP ingress,
- Distributed worker runtime coordination and polling,
- PostgreSQL connection pooling and socket timeouts,
- Public API and worker security credentials,
- Engine-level operational retries and OCC contention backoffs,
- Operational deadlines (execution-start, cancellation resolution, worker liveness),
- Ingress payload admission limits and structural complexity guards,
- Structured logging, OpenTelemetry tracing, and Prometheus metrics,
- Local developer ergonomics, CI pipelines, and Docker Compose reference deployments,

while ensuring that:
- Configuration parameterizes approved mechanisms without redefining architectural invariants,
- In-flight execution contracts are never retroactively corrupted by process restarts,
- Sensitive credentials cannot be deployed with hardcoded insecure defaults,
- Configuration is strongly validated and immutable for the lifetime of the process, and
- The system remains operable and maintainable by a solo engineer?

---

## 4. Requirements Covered

- **REQ-CFG-001 (Invariant Non-Configurability)**: Core orchestration semantics, state machines, dependency satisfaction rules, and persistence atomicity must not be alterable via configuration.
- **REQ-CFG-002 (Type-Safe Startup Validation)**: All configuration settings must be strongly typed and validated at process initialization using Pydantic v2 / `pydantic-settings`.
- **REQ-CFG-003 (Process Lifetime Immutability)**: The configuration snapshot must remain immutable for the lifetime of a running process; dynamic runtime hot-reloading is deferred.
- **REQ-CFG-004 (Durability of Semantic & Resolved Timing Values)**: Effective values that define execution contracts (e.g., `max_attempts`) or timing deadlines (e.g., `start_deadline_utc`, `retry_ready_at`) must be stored durably on owning database records and never recomputed from current process defaults during recovery.
- **REQ-CFG-005 (Secure Credential Handling)**: Passwords, tokens, and private keys must be treated as protected secret types (`SecretStr`), injected via environment or mounted files, and never possess hardcoded defaults.
- **REQ-CFG-006 (Single Control-Plane Invariant Enforcement)**: Server configuration must detect and forbid local multi-process application worker configurations ($N=1$).
- **REQ-CFG-007 (Deterministic Precedence)**: Configuration resolution must follow an unambiguous, deterministic source hierarchy.

---

## 5. Constraints

- **Single Authoritative Process ($N=1$)**: NexusFlow V1 assumes an in-process ephemeral Worker Registry and local scheduler coordination. Configuration must not support multi-process control-plane execution before [ADR-025](docs/architecture/adr-025-high-availability-and-clustering.md).
- **No External Distributed Config Store in V1**: Technologies such as Consul, etcd, ZooKeeper, or Redis-backed dynamic configuration are excluded.
- **Python 3.12 & Pydantic v2**: Configuration must leverage `pydantic-settings` natively within Python 3.12 without introducing secondary configuration parsers.
- **No IWS Schema Expansion**: ADR-023 must not invent new public workflow-definition schema fields (e.g., public task retry-delay backoff specifications) that were not established in [ADR-001](docs/architecture/adr-001-internal-workflow-specification.md) or [ADR-007](docs/architecture/adr-007-task-execution-lifecycle-and-execution-attempt-model.md).

---

## 6. Goals

- Define a rigorous taxonomy separating Architectural Invariants, Definition Semantic Values, Operational Policies Resolved Durably, Process Operational Settings, and Deployment Secrets.
- Specify a centralized, typed settings model built with Pydantic v2.
- Enforce fail-closed validation rules for structural types, cross-field constraints, and credential domain segregation prior to ingress readiness.
- Establish an unambiguous source precedence model protecting against accidental configuration overriding.
- Document baseline initial V1 implementation defaults as tunable candidates.
- Define the exact restart change matrix illustrating how configuration updates interact with in-flight versus newly created entities.

---

## 7. Non-Goals

- **No Dynamic Runtime Hot Reloading**: Configuration cannot be modified on a running process without a process restart.
- **No Distributed Configuration Synchronization**: Multi-node configuration propagation is deferred to [ADR-025](docs/architecture/adr-025-high-availability-and-clustering.md).
- **No Dynamic Feature Flag Platform**: Configuration does not provide a runtime feature-toggling or A/B testing framework.
- **No Application-Level Orchestration Quotas**: Distributed task concurrency caps and rate-limiting scheduling queues remain deferred.
- **No External Configuration CLI**: NexusFlow does not provide an independent administrative configuration CLI tool in V1.

---

## 8. Candidate Solutions

### Candidate A: Hardcoded Module Constants
- Operational parameters are fixed as Python constants in module files.
- **Evaluation**: Prevents deployment across varying operational environments (local development, CI, Docker Compose, production) without code modifications.

### Candidate B: Untyped Environment Variable Access (`os.environ`)
- Modules read environment variables directly at point of use with ad-hoc casting and fallback defaulting.
- **Evaluation**: Highly error-prone, scatters configuration keys across the codebase, lacks centralized validation, and allows typos to silently fall back to defaults.

### Candidate C: Typed, Validated, Immutable-at-Startup Configuration via Pydantic v2 (Selected)
- Centralized settings models structured with `pydantic-settings`.
- Enforces strict typing, field bounds, and cross-field validation at startup.
- Freezes configuration for the process lifetime.
- Integrates cleanly with Pydantic `SecretStr` for credential protection.
- Maintains clear separation between ephemeral operational parameters and durable semantic execution values.

### Candidate D: External Distributed Key-Value Store (Consul / etcd)
- Control plane and workers pull dynamic configuration from an external consensus cluster with change-watch listeners.
- **Evaluation**: Excessive operational complexity and infrastructure overhead that violates solo-developer constraints and introduces distributed race conditions into a single-instance V1 engine.

---

## 9. Detailed Evaluation

| Dimension | Candidate A (Hardcoded) | Candidate B (Untyped `os.environ`) | Candidate C (Typed Pydantic Settings) | Candidate D (Distributed Config) |
| :--- | :--- | :--- | :--- | :--- |
| **Portability across Envs** | None | High | **High (Env & File Overrides)** | High |
| **Type Safety & Validation**| High (Compile time) | None (Runtime string parsing) | **Comprehensive (Pydantic v2)** | External Schema Dependent |
| **Architectural Protection**| Rigid | Dangerous (Ad-hoc overrides) | **Enforced (Invariant Non-Configurability)** | Risky (Runtime mutation) |
| **Operational Complexity** | None | Low | **Low (Solo Developer Ergonomic)** | Prohibitive for V1 |
| **Secret Masking** | Poor (Code leaks) | Poor | **Built-in (`SecretStr` masking)** | External Vault Dependent |
| **Execution Determinism** | High | Low (Silent fallback errors) | **High (Immutable Process Snapshot)** | Low (Mid-flight drift) |

---

## 10. Decision

NexusFlow V1 adopts a **Typed, Validated, Immutable-at-Startup Configuration Architecture implemented with Pydantic v2 and `pydantic-settings`**:

1. **Central Principle**:
   > **Configuration parameterizes approved mechanisms; it does not create new semantics.**
   Configuration tunes operational thresholds, connection pools, and timing durations. It is strictly prohibited from altering or disabling approved architectural invariants.
2. **Configuration Classification**: All settings and system parameters are classified into five explicit categories:
   - **Architectural Invariants**: Immutable rules enforced in code; never configurable.
   - **Definition Semantic Values**: Values that define execution contracts (e.g., `max_attempts`); stored durably within `ValidatedIWS`.
   - **Operational Policy Resolved Durably**: Process-configured durations whose effective values materialize as absolute UTC timestamps (e.g., `start_deadline_utc`, `retry_ready_at`) on owning entity records at the moment of state transition.
   - **Process Operational Settings**: Ephemeral parameters (e.g., database pool size, listen port, log level) tuning process execution; safe to change across process restarts.
   - **Deployment / Secret Inputs**: Environment credentials injected via protected environment variables or mounted secret files; no hardcoded defaults.
3. **Immutable Process Snapshot**: Configuration is validated and loaded into a frozen snapshot during process startup. Dynamic runtime hot-reloading is deferred.
4. **Fail-Closed Startup Validation**: Missing required fields, malformed types, negative intervals, cross-field inconsistencies, or credential domain overlaps immediately halt process startup with a non-zero exit code and sanitized diagnostics. The service never enters a ready state with invalid configuration.
5. **Single Control-Plane Invariant Enforced ($N=1$)**: Server configuration prohibits local multi-process execution (e.g., Uvicorn `--workers > 1` or Gunicorn master-worker topologies). Multi-container replica deployment is prohibited as an operational constraint until [ADR-025](docs/architecture/adr-025-high-availability-and-clustering.md).
6. **Task vs. Engine Retry Separation**: Business task retries (which consume attempt budgets and transition to `RETRY_WAIT`) are strictly separated from internal engine operational retries (which handle transient DB drops or OCC revision conflicts without consuming attempt budgets).

---

## 11. Decision Rationale

### 11.1 Principle: Parameterization vs. Semantic Mutation
Allowing configuration to redefine core behavior (such as making task dependency satisfaction optional, bypassing OCC revision guards, or disabling history logging) destroys architectural integrity. Invariants must be enforced uniformly in code; configuration exists solely to adjust operational parameters (such as connection pool capacities, network timeouts, and log filtering thresholds).

### 11.2 Separation of Semantic Durability from Ephemeral Settings
If an operator updates a process-level default (e.g., changing the default task execution timeout from 60 seconds to 30 seconds), applying that change to an in-flight workflow that was registered and accepted under the 60-second rule violates execution contracts and breaks determinism. By embedding semantic parameters into `ValidatedIWS` and resolving operational durations into absolute UTC timestamps at transition time, NexusFlow guarantees that process restarts never retroactively alter in-flight execution contracts.

### 11.3 Enforcing the Single-Process Invariant
NexusFlow V1 relies upon an in-process ephemeral Worker Registry and local scheduler loops. If an operator configures Uvicorn with `--workers 4`, the operating system forks four separate processes, resulting in four isolated, competing worker registries and scheduler instances running against the same database without clustering coordination. Explicitly validating and enforcing `server.process_count == 1` at boot prevents catastrophic split-brain scheduling.

---

## 12. Tradeoffs

- **Immutable Snapshot vs. Hot Reload**:
  - *Tradeoff*: Any configuration adjustment requires a process restart.
  - *Benefit*: Improves reproducibility, eliminates concurrency races across active background loops, prevents partially applied configuration states, and simplifies testing.
- **Single-Process Constraint vs. Multi-Core Utilization**:
  - *Tradeoff*: The control-plane process cannot scale across CPU cores via ASGI worker forking.
  - *Benefit*: Preserves the correctness of the in-process Worker Registry and scheduler coordination without introducing distributed consensus overhead before ADR-025.
- **Persisted Absolute Deadlines vs. Dynamic Tuning**:
  - *Tradeoff*: Changing a process-level timeout duration does not accelerate or extend active, in-flight attempts.
  - *Benefit*: Guarantees that execution timing rules remain deterministic and immune to configuration drift during recovery.

---

## 13. Consequences

### 13.1 Positive Consequences
- **Architectural Safeguards**: Configuration cannot be used to bypass state machines, disable audit history, or weaken security boundaries.
- **Early Failure Detection**: Configuration errors fail immediately at boot before the control plane touches PostgreSQL or accepts external traffic.
- **Deterministic Recovery**: Crash recovery evaluates persisted timestamps and durable IWS models without consulting altered process defaults.
- **Ergonomic Developer Experience**: Local development functions smoothly via `.env`, while production environments remain strictly controlled via environment variables.

### 13.2 Negative Consequences & Liabilities
- **Process Restart Required**: Updating operational parameters (such as connection pool sizes) requires a brief control-plane restart.
- **Configuration Maintenance Overhead**: All operational knobs must be declared in strongly typed Pydantic models with explicit validation rules.
- **External Multi-Replica Limitation**: Configuration validation can enforce `process_count == 1` locally, but cannot universally detect independently spawned external container replicas without distributed leasing (deferred to ADR-025).

---

## 14. Failure Modes

### 14.1 Missing Required Configuration
- *Symptom*: Control plane fails to boot; logs indicate `ValidationError: database.url: Field required`.
- *Handling*: Process terminates immediately with non-zero exit code. Readiness probe never passes.

### 14.2 Cross-Field Validation Failure
- *Symptom*: Operator configures `heartbeat_interval_seconds = 10` and `liveness_timeout_seconds = 10`.
- *Handling*: Validator raises `ValueError: liveness_timeout_seconds must be greater than heartbeat_interval_seconds`. Process terminates.

### 14.3 Multi-Process Uvicorn Misconfiguration
- *Symptom*: Operator launches Uvicorn with `--workers 4`.
- *Handling*: Server startup hook asserts `process_count == 1`; detects multi-worker environment, logs a fatal diagnostic, and halts immediately.

### 14.4 Credential Domain Overlap
- *Symptom*: Operator inadvertently configures the public API Bearer token and the worker Bearer token with identical secret strings.
- *Handling*: Startup validator computes SHA-256 digests, detects matching values across domains, logs a fatal security configuration error, and aborts startup.

---

## 15. Debugging

- **Sanitized Startup Summary**: Upon successful validation and boot, the control plane logs a structured configuration summary at `INFO` level.
- **Strict Masking**: Secret fields (`database.url` credentials, public Bearer tokens, worker Bearer tokens, TLS private keys) are logged strictly as `<redacted>` or `<configured>`. Raw tokens or password strings are never printed.
- **Clear Error Attribution**: Configuration validation errors identify the specific key name, the supplied type, and the violated constraint without revealing secret values.

---

## 16. Testing

Per [ADR-021](docs/architecture/adr-021-testing-strategy.md), configuration architecture is verified directly within test harnesses:

```
tests/
├── unit/
│   ├── test_config_validation.py    # Type checking, positive bounds, cross-field rules
│   ├── test_config_precedence.py    # Source hierarchy: Env > Secret Files > Defaults
│   └── test_secret_masking.py       # Assert repr() and str() mask SecretStr values
├── integration/
│   ├── test_startup_lifecycle.py    # Verify startup aborts on missing required fields
│   ├── test_single_process_guard.py # Verify multi-worker server config is rejected
│   └── test_credential_domains.py   # Verify identical public/worker tokens are rejected
└── recovery/
    └── test_durable_deadlines.py    # Verify restart uses persisted absolute deadlines
```

- **Zero Test-Bypass Flags**: The configuration schema explicitly forbids test-only bypass flags (e.g., `skip_auth_for_tests=true` or `simulate_db_failure=true` are strictly prohibited in production models).
- **Programmatic Injection**: Test harnesses inject synthetic configuration instances directly into component constructors or use scoped fixtures without polluting host environment variables.

---

## 17. Operational Considerations

- **Secret Management**: Production deployments supply credentials (`DATABASE_URL`, public API tokens, worker tokens) via container environment variables or mounted secret files. Plaintext secrets must never be committed to Git or baked into container images.
- **Controlled Credential Rotation**: The security settings model supports configuring multiple public API principals and multiple active worker-domain credential digests simultaneously, enabling zero-downtime credential rotation across a maintenance window. Applying updated credentials requires a process restart in V1.
- **Local Developer Experience**: Bare local development defaults server binding to loopback (`127.0.0.1`) and permits `.env` file loading. Docker Compose reference environments explicitly configure `0.0.0.0` and disable `.env` auto-loading.
- **Readiness Probes**: The control-plane readiness endpoint reflects process health and startup completion; it remains unready until configuration validation, database connection, schema migration verification, and startup recovery reconciliation have succeeded.

---

## 18. Maintenance

- **Adding Configuration Parameters**: New operational parameters must be added to the appropriate sub-model in `NexusFlowSettings` with explicit type annotations, safe defaults, and validation constraints.
- **Documentation Generation**: Supported configuration parameters, types, defaults, and governing ADR references must be maintained in a centralized configuration reference document.
- **Deprecation Policy**: Renamed or deprecated configuration keys must generate descriptive warnings during validation before being removed in subsequent releases.

---

## 19. Future Evolution

- **ADR-024 (Workflow Versioning)**: Configuration will support default execution versioning strategies and migration policies for active definitions.
- **ADR-025 (High Availability & Clustering)**: Multi-node control-plane deployments will introduce distributed leader election, shared configuration synchronization, and cluster-wide worker registries, superseding the local $N=1$ process constraint.
- **ADR-026 (Client & Worker SDKs)**: Worker and client SDKs will provide typed configuration builders mirroring control-plane timeout and reconnect parameters.
- **Enterprise Secret Stores**: Future releases may introduce native integration plugins for HashiCorp Vault, AWS Secrets Manager, or Kubernetes Secrets API.

---

## 20. Rejected Alternatives

- **Rejecting Dynamic Runtime Hot-Reloading in V1**: Modifying timeouts, pool sizes, or coordination intervals mid-flight introduces severe race conditions and partial configuration states across active background tasks.
- **Rejecting External Distributed Config Stores (Consul / etcd)**: Adds unnecessary operational friction, external failure points, and deployment overhead for a solo-developer V1 architecture.
- **Rejecting Separate Application YAML/TOML Configuration Files**: Having two different YAML formats (one for workflow definitions and one for application config) creates operator confusion. Environment variables provide standard, container-native configuration.
- **Rejecting Insecure Default Credentials**: Default passwords or fallback API tokens create severe production security risks; fail-closed credential requirements eliminate this failure class.
- **Rejecting Global Task Concurrency Knobs**: Workflow and task scheduling quotas remain deferred; introducing coarse global throttling knobs without scheduler queue architecture would violate ADR-005 invariants.

---

## 21. Decision Evolution

- **Initial Concept**: Ad-hoc environment variable parsing scattered across modules with hardcoded defaults.
- **Architectural Calibration**: Recognized that asynchronous orchestration requires strict determinism. Adopted typed, immutable Pydantic settings models.
- **Durability Separation**: Established the vital distinction between ephemeral process settings and durable semantic values, ensuring that process restarts cannot retroactively modify accepted workflow execution rules.
- **Process Invariant Enforcement**: Added explicit validation guarding against multi-process ASGI worker spawning to protect the in-process Worker Registry.

---

## 22. Common Misconceptions

- *Misconception*: "I can change the task execution timeout in configuration to speed up stuck in-flight tasks."  
  *Correction*: Task execution timeouts are resolved into durable absolute UTC deadlines upon attempt creation. Modifying configuration defaults affects only future attempts; in-flight attempts preserve their accepted contractual deadlines.
- *Misconception*: "Running Uvicorn with `--workers 4` will scale my control plane across CPU cores."  
  *Correction*: Multi-worker execution spawns four separate processes, each with an isolated in-process Worker Registry and competing scheduler loops. NexusFlow V1 requires exactly one control-plane process ($N=1$).
- *Misconception*: "Using Pydantic `SecretStr` means our process memory is cryptographically encrypted."  
  *Correction*: `SecretStr` prevents accidental disclosure in logs, exception traces, and string serialization. The underlying plaintext is necessarily accessible in process memory when evaluating credentials or establishing database connections.
- *Misconception*: "Configuration can make task dependencies optional or allow failures to be ignored."  
  *Correction*: Dependency satisfaction is an immutable architectural invariant governed by ADR-005. Configuration can only tune operational parameters, never lifecycle truth.

---

## 23. Open Questions

- *None*. The configuration architecture establishes a complete, robust, and minimal framework fully aligned with ADR-001 through ADR-022.

---

## 24. Interview Discussion

In systems engineering interviews, NexusFlow's configuration architecture illustrates how to parameterize distributed systems without sacrificing determinism:
- It highlights the critical separation between **ephemeral operational configuration** (which can change safely across restarts) and **durable semantic configuration** (which must be resolved and persisted with domain entities to prevent retroactively altering execution rules).
- It demonstrates rigorous architectural protection: configuration is strictly forbidden from acting as a backdoor to disable state machines, bypass OCC revision guards, or weaken transactional consistency groups.
- It illustrates practical systems discipline: enforcing the single-process invariant ($N=1$) at startup to protect the in-process Worker Registry, and eliminating insecure default credentials through fail-closed validation.

---

## 25. References

- [ADR-001: Internal Workflow Specification (IWS)](docs/architecture/adr-001-internal-workflow-specification.md)
- [ADR-003: Canonical Workflow Graph Representation](docs/architecture/adr-003-canonical-workflow-graph.md)
- [ADR-004: Workflow Validation Strategy](docs/architecture/adr-004-semantic-validation.md)
- [ADR-005: Workflow Task Scheduling & Dispatch Architecture](docs/architecture/adr-005-task-scheduling-and-eligibility.md)
- [ADR-006: Workflow Execution State Machine](docs/architecture/adr-006-workflow-execution-state-machine.md)
- [ADR-007: Task Execution Lifecycle & Attempt Model](docs/architecture/adr-007-task-execution-lifecycle-and-execution-attempt-model.md)
- [ADR-008: Worker Coordination & Liveness Model](docs/architecture/adr-008-worker-coordination-and-liveness.md)
- [ADR-009: Task Routing Strategy](docs/architecture/adr-009-task-routing.md)
- [ADR-010: Workflow Data Flow & Parameter Passing](docs/architecture/adr-010-workflow-data-flow-and-parameter-passing.md)
- [ADR-011: State Persistence Strategy](docs/architecture/adr-011-state-persistence.md)
- [ADR-012: Recovery Strategy](docs/architecture/adr-012-recovery.md)
- [ADR-013: Consistency & Concurrency Strategy](docs/architecture/adr-013-consistency-and-concurrency.md)
- [ADR-014: Execution History & Audit Model](docs/architecture/adr-014-execution-history-and-audit-model.md)
- [ADR-015: External API Architecture](docs/architecture/adr-015-external-api-architecture.md)
- [ADR-016: Observability Architecture](docs/architecture/adr-016-observability.md)
- [ADR-017: Graceful Shutdown Strategy](docs/architecture/adr-017-graceful-shutdown-architecture.md)
- [ADR-018: Error Handling Philosophy](docs/architecture/adr-018-error-handling-philosophy.md)
- [ADR-019: Project Modularity & Service Boundaries](docs/architecture/adr-019-project-and-service-boundaries.md)
- [ADR-020: Technology Selection Strategy](docs/architecture/adr-020-technology-selection.md)
- [ADR-021: Testing Strategy](docs/architecture/adr-021-testing-strategy.md)
- [ADR-022: Security Architecture](docs/architecture/adr-022-security-architecture.md)
- Pydantic Settings Documentation: https://docs.pydantic.dev/latest/concepts/pydantic_settings/

---

## 26. Traceability

### 26.1 Traceability to Architecture & Requirements

| Requirement / Invariant | Configuration Mechanism | Enforcement Layer | Verification Reference |
| :--- | :--- | :--- | :--- |
| **REQ-CFG-001** | Immutable architectural invariants non-configurable | Core Domain Entities | Unit tests asserting absence of configuration hooks |
| **REQ-CFG-002** | Strongly typed settings via `pydantic-settings` | Settings Models | `tests/unit/test_config_validation.py` |
| **REQ-CFG-003** | Frozen settings snapshot for process lifetime | Root Settings Instance | Immutability unit tests |
| **REQ-CFG-004** | Semantic values persisted; absolute deadlines stored | PostgreSQL Entities | `tests/recovery/test_durable_deadlines.py` |
| **REQ-CFG-005** | `SecretStr` masking; zero default credentials | Security Settings | `tests/unit/test_secret_masking.py`, `test_credential_domains.py` |
| **REQ-CFG-006** | Local single control-plane process guard ($N=1$) | Server Ingress Adapter | `tests/integration/test_single_process_guard.py` |
| **REQ-CFG-007** | Deterministic precedence (Env > Files > Defaults) | Settings Loader | `tests/unit/test_config_precedence.py` |
| **ADR-008 / 022**| Credential domain segregation; public $\ne$ worker | Settings Validators | `tests/integration/test_credential_domains.py` |
| **ADR-013** | OCC bounded operational retry configuration | Engine Settings | `tests/integration/test_concurrency.py` |
| **ADR-016** | Telemetry fail-open; non-blocking OTLP exporter | Observability Settings | `tests/integration/test_telemetry_redaction.py` |

---

## 27. Decision Validation Checklist

- [x] **Separation of Concerns**: Architectural invariants are strictly non-configurable.
- [x] **Durability Model Enforced**: Definition semantics reside in `ValidatedIWS`; operational policies resolved into absolute UTC timestamps on entity records.
- [x] **Typed Settings via Pydantic v2**: `pydantic-settings` used for all settings definitions.
- [x] **Process Lifetime Immutability**: Configuration snapshot is frozen at startup; dynamic hot reload is deferred.
- [x] **Fail-Closed Validation**: Invalid types, bounds, or cross-field constraints abort startup immediately before readiness.
- [x] **Zero Insecure Defaults**: No hardcoded API tokens, worker credentials, or database passwords exist.
- [x] **Single-Process Constraint Enforced**: Local multi-worker ASGI execution is detected and rejected ($N=1$).
- [x] **Task vs. Engine Retries Separated**: Task attempt budgets are decoupled from internal OCC and infrastructure retries.
- [x] **Clear Precedence Hierarchy**: Explicit test injection > environment variables > mounted secrets > local `.env` > code defaults.
- [x] **Full Traceability Maintained**: Fully traced to all preceding platform ADRs (ADR-001 through ADR-022).
