# ADR-022: Security Architecture

## 1. Purpose

This Architecture Decision Record (ADR) establishes the security architecture, trust boundaries, threat model, and cryptographic verification framework for NexusFlow V1. It defines how public control-plane API access, distributed worker coordination, database access, operational telemetry, and external inputs are secured without altering previously approved orchestration, scheduling, lifecycle, persistence, or recovery invariants.

---

## 2. Context

NexusFlow V1 operates as a modular monolith control plane (FastAPI, Uvicorn, Python 3.12, SQLAlchemy 2.0 Async, `asyncpg`, PostgreSQL 16) coordinating with external distributed Python V1 workers over an HTTP/JSON pull/long-poll protocol ([ADR-019](docs/architecture/adr-019-project-and-service-boundaries.md), [ADR-020](docs/architecture/adr-020-technology-selection.md)). Orchestration state truth is maintained under `READ COMMITTED` isolation with integer-revision Optimistic Concurrency Control (OCC) ([ADR-013](docs/architecture/adr-013-consistency-and-concurrency.md)). Worker coordination follows a two-phase Candidate $\to$ Ownership handshake ([ADR-008](docs/architecture/adr-008-worker-coordination-and-liveness.md)), execution history provides immutable audit trails ([ADR-014](docs/architecture/adr-014-execution-history-and-audit-model.md)), public operations expose REST contracts ([ADR-015](docs/architecture/adr-015-external-api-architecture.md)), and layered invariant testing verifies engine correctness ([ADR-021](docs/architecture/adr-021-testing-strategy.md)).

Distributed asynchronous orchestration systems present distinct security challenges:
- External callers must be authenticated before submitting workflows, inspecting state, or issuing cancellations.
- Distributed workers execute on remote compute nodes and must prove legitimate membership in the worker security domain before polling tasks or reporting execution results.
- Sensitive business payloads and database credentials must not leak into logs, traces, or unauthenticated health/metrics scrapers.
- Malicious or malformed inputs (YAML definition bombs, oversized JSON payloads, injection attempts) must be neutralized at boundary adapters before consuming system resources.
- Operational security must remain tractable for a solo engineer and reproducible across local development, CI, and reference Docker Compose environments.

---

## 3. Problem Statement

How should NexusFlow V1 secure:
- Public control-plane API access,
- Distributed worker-to-control-plane communication,
- Worker identity and execution authority,
- Authorization across administrative, operational, and execution contexts,
- Ownership-sensitive worker callbacks and replay windows,
- Transport confidentiality and integrity,
- Database credentials and access boundaries,
- Workflow definition ingestion and business data payloads,
- Operational logs, traces, and metrics,
- Supply-chain dependencies and container artifacts,

while preserving all previously approved lifecycle state machines, transactional consistency groups, worker coordination protocols, and crash recovery semantics?

---

## 4. Requirements Covered

- **REQ-SEC-001 (Credential Domain Separation)**: Public API clients and distributed workers must authenticate using distinct, non-interchangeable credential domains.
- **REQ-SEC-002 (Transport Security)**: All communication crossing untrusted or externally exposed network boundaries must use TLS.
- **REQ-SEC-003 (Constant-Time Verification)**: Machine-to-machine bearer credentials must be verified using constant-time digest comparison to reduce timing side-channel leakage.
- **REQ-SEC-004 (Isolation of DB Credentials)**: Database credentials must be restricted strictly to the control plane process; workers must have zero direct database access or credentials.
- **REQ-SEC-005 (Safe Input & Deserialization)**: Workflow YAML definitions and JSON payloads must be parsed in safe, non-evaluating modes with bounded byte sizes and structural complexity limits.
- **REQ-SEC-006 (Structured Telemetry Redaction)**: Diagnostic logs, Prometheus metrics, and OpenTelemetry traces must systematically redact authorization headers, passwords, and sensitive business data.
- **REQ-SEC-007 (Fail-Closed Security Operations)**: Unauthenticated or unauthorized requests must fail immediately without altering domain state, scheduling attempts, or writing history.
- **REQ-SEC-008 (Authority vs. Authentication Separation)**: Execution authority (`WorkerSessionId`, `AttemptId`) must be strictly decoupled from security identity authentication.

---

## 5. Constraints

- **Single Control-Plane Deployment Model**: NexusFlow V1 deploys as a single control-plane process with external distributed workers; enterprise identity providers (IdPs), OAuth2/OIDC servers, and dynamic PKI/CA infrastructure are out of scope for V1.
- **No Multi-Tenancy in V1**: Single administrative and security domain per deployment; no `TenantId` or tenant data isolation models exist.
- **Standard Python 3.12 Tooling**: Security mechanisms must use standard library primitives (`secrets`, `hmac.compare_digest`, `hashlib`) rather than heavy external security frameworks.
- **No Orchestration Semantics Alteration**: Security mechanisms must not introduce synthetic lifecycle states into domain state machines, modify OCC revision semantics, or bypass transactional consistency groups.
- **No Worker Process Sandbox in V1**: Worker activity handlers execute in a worker-local thread pool; V1 does not provide operating system process or container sandboxing for mutually untrusted activity code.

---

## 6. Goals

- Define explicit trust boundaries, assets, and a calibrated threat model across all architectural interfaces.
- Establish distinct, high-entropy Bearer credential domains for public API clients and distributed worker deployments.
- Enforce constant-time SHA-256 digest comparison for machine token verification.
- Enforce least-privilege database connectivity and network isolation.
- Specify safe parsing modes and bounded ingestion guards for YAML definitions and JSON payloads.
- Define structured secret redaction across diagnostic logs, distributed traces, and Prometheus metrics.
- Maintain full traceability to governing ADRs and test requirements in [ADR-021](docs/architecture/adr-021-testing-strategy.md).

---

## 7. Non-Goals

- **No Multi-Tenancy**: ADR-022 does not introduce organization, workspace, or tenant isolation concepts.
- **No Personal User Account System**: V1 does not support human user accounts, passwords, login sessions, or password-reset flows.
- **No Mandatory OAuth2 / OIDC in V1**: External identity federation is deferred.
- **No Mandatory Mutual TLS (mTLS) in V1**: mTLS is deferred as an optional deployment-hardening strategy; it is not required for standard V1 operation.
- **No Untrusted Activity Sandboxing**: V1 does not guarantee memory or filesystem isolation between user activity code and the worker runtime process.
- **No Application-Level Encryption at Rest**: Volume and disk encryption remain operational/deployment responsibilities.
- **No Application Rate-Limiting Semantics**: Orchestration scheduling remains unaffected by network-level rate limits.

---

## 8. Candidate Solutions

### Candidate A: Trusted-Network / Zero Authentication
- Relies entirely on private network boundaries (VPC or Docker bridge) with no application-level authentication.
- **Evaluation**: Unacceptable. Any compromised container, misplaced port exposure, or insider on the network can forge arbitrary worker callbacks, claim tasks, or alter execution history.

### Candidate B: Static Shared Secrets Everywhere
- A single pre-shared secret token shared interchangeably across public API clients, operators, and workers.
- **Evaluation**: Fails privilege segregation. A compromise of any worker node grants full administrative control over workflow definition registration, arbitrary execution cancellation, and history reads.

### Candidate C: Pragmatic Defense-in-Depth with Distinct Credential Domains (Selected)
- Implements distinct high-entropy Bearer credentials for public API clients and distributed worker deployments.
- Verifies credentials using constant-time SHA-256 digest comparison.
- Mandates TLS across untrusted network boundaries.
- Separates security authentication from orchestration authority (`WorkerSessionId`, `AttemptId`).
- Restricts database access strictly to the control plane under least-privilege roles.
- Enforces safe YAML/JSON parsing, structural bounds, and structured telemetry redaction.
- Documents a trusted-code assumption for in-process worker activity handlers.

### Candidate D: Full Enterprise IAM & Sandboxed Mesh (OAuth2/OIDC + mTLS + Container Sandboxing)
- Integrates external IdP delegation, JWT verification, automated x509 PKI for worker mTLS, and container-per-task execution sandboxes.
- **Evaluation**: Massive operational complexity and infrastructure overhead that violates solo-developer constraints and delays delivery without providing additional correctness value for the V1 architecture.

---

## 9. Detailed Evaluation

| Dimension | Candidate A (Zero Auth) | Candidate B (Single Shared Secret) | Candidate C (Pragmatic Defense-in-Depth) | Candidate D (Full Enterprise IAM) |
| :--- | :--- | :--- | :--- | :--- |
| **Privilege Segregation** | None | None (single blast radius) | **High (distinct public & worker domains)**| Very High (fine-grained IAM) |
| **Operational Complexity** | Very Low | Low | **Low / Sustainable for Solo Developer** | Prohibitive for V1 |
| **Transport Security** | None | Ad-hoc | **Enforced (TLS on untrusted boundaries)** | Enforced (mTLS mesh) |
| **Worker Identity** | None | Shared Secret | **Worker Security Domain Credential** | Cryptographic Workload Identity |
| **Implementation Footprint**| Zero | Minimal | **Standard Library (`secrets`, `hmac`)** | Heavy (IdP, PKI, Vault) |
| **Fidelity to Prior ADRs** | Compromises audit/auth | Compromises boundaries | **Full fidelity to ADR-001–021** | Modifies operational topology |

---

## 10. Decision

NexusFlow V1 adopts a **Pragmatic Defense-in-Depth Security Architecture**:

1. **Distinct Credential Domains**: Machine-to-machine authentication is enforced via distinct, high-entropy Bearer API credentials for public API clients and distributed worker deployments. Public credentials cannot access worker coordination endpoints; worker credentials cannot invoke public administrative operations.
2. **Constant-Time Digest Verification**: High-entropy Bearer tokens are verified using SHA-256 digest matching via `hmac.compare_digest` to reduce timing side-channel leakage.
3. **Separation of Authentication from Orchestration Authority**: Security authentication proves caller membership in an authorized security domain. Orchestration authority (`WorkerSessionId`, `AttemptId`, OCC revision) is evaluated separately according to operation-specific lifecycle rules. `WorkerSessionId` and `AttemptId` are coordination tokens, **not** security credentials.
4. **Worker Security Domain Model**: Distributed workers authenticate using a shared worker-security-domain credential. This credential proves membership in the trusted worker deployment; it does not provide cryptographic per-process identity.
5. **Transport Security (TLS)**: TLS is mandatory for all network traffic crossing untrusted or externally exposed boundaries. Local plaintext communication is permitted strictly within explicitly isolated development environments.
6. **Least-Privilege Database Access**: Only the control plane process holds PostgreSQL credentials. Workers have zero database connectivity and no database credentials.
7. **Safe Input & Deserialization**: Workflow YAML definitions and JSON payloads are processed in safe, non-evaluating modes with bounded document sizes and structural complexity limits. No dynamic Python code execution or shell execution occurs in the control plane.
8. **Structured Redaction**: Telemetry formatters automatically redact authorization headers, passwords, and sensitive payloads from logs, traces, and metrics.
9. **Trusted-Code Assumption for Workers**: Activity handlers run in the worker-local thread pool without process or memory sandboxing. User activity code is assumed to be trusted within the worker deployment boundary.
10. **Fail-Closed Enforcement**: Unauthenticated or unauthorized operations fail immediately without modifying orchestration state, scheduling attempts, or generating semantic history entries.

---

## 11. Decision Rationale

### 11.1 Principle: Authentication Is Not Orchestration Authority
Orchestration systems must clearly separate *identity verification* from *state transition authority*:
- **Authentication** checks whether the caller possesses a valid Bearer token for the requested endpoint domain.
- **Orchestration Authority** checks whether that authenticated caller is permitted to mutate a specific entity based on lifecycle rules, `(AttemptId, WorkerSessionId)` correlation, and OCC revision $N$.
- Conflating these concepts leads to severe vulnerabilities: treating `WorkerSessionId` as a credential allows anyone who guesses or observes a UUID to hijack an execution; conversely, relying solely on Bearer tokens without attempt correlation allows stale workers to overwrite newer attempts.

### 11.2 Principle: Machine Tokens vs. Human Passwords
NexusFlow V1 interfaces exclusively through machine-to-machine APIs. Bearer credentials are cryptographically secure random tokens drawn with 256 bits of entropy. Because these tokens are not human-selected passwords, slow password-hashing algorithms (e.g., Argon2, bcrypt) designed for offline dictionary attack resistance are unnecessary and introduce needless CPU latency on every API request. Fast SHA-256 digest matching combined with `hmac.compare_digest` provides appropriate protection for high-entropy machine secrets.

### 11.3 Principle: Worker Security Domain Blast Radius
In V1, distributed workers run within an organizationally managed environment. Sharing a dedicated worker bearer credential across workers in a deployment provides adequate transport authentication and segregation from public APIs without imposing the substantial burden of managing a private certificate authority (CA) or per-worker credentials. The residual blast radius—that a compromised worker environment allows impersonating the worker security domain—is explicitly documented and mitigated by control-plane lifecycle and OCC guards.

---

## 12. Tradeoffs

- **Static Bearer Tokens vs. OAuth2/OIDC**:
  - *Tradeoff*: Lacks dynamic user provisioning, single sign-on (SSO), and centralized token issuance.
  - *Benefit*: Self-contained, zero external runtime dependencies, low operational overhead, and straightforward machine-to-machine automation.
- **Shared Worker-Domain Credential vs. mTLS / SPIFFE**:
  - *Tradeoff*: Does not cryptographically isolate individual worker operating-system processes; a compromised worker credential allows deployment-wide worker protocol access.
  - *Benefit*: Completely eliminates PKI/CA issuance complexity, certificate expiration outages, and severe local developer friction.
- **Thread-Pool Activity Execution vs. Container Sandboxing**:
  - *Tradeoff*: Activity handlers share memory and process environment with the worker runtime; malicious code can read the worker credential.
  - *Benefit*: Avoids container-daemon management, minimizes task dispatch latency, and operates seamlessly across standard developer environments.
- **Digest Verification vs. Plaintext Storage**:
  - *Tradeoff*: Minor CPU cost on configuration load to derive digests.
  - *Benefit*: Prevents the application database from persisting plaintext credentials.

---

## 13. Consequences

### 13.1 Positive Consequences
- **Clean Architectural Layering**: Authentication is enforced at security/ingress adapters, while authorization is evaluated at ingress and application policy boundaries; domain state machines remain entirely framework-independent.
- **Strict Credential Domain Isolation**: Public clients cannot interact with worker coordination endpoints; workers cannot trigger administrative API actions.
- **Deterministic Testability**: All security mechanisms are testable via standard pytest harnesses using synthetic credentials without mocking external authentication providers.
- **Telemetry Sanitization**: Structured redaction prevents accidental credential leakage into logs, metrics, and distributed traces.

### 13.2 Negative Consequences & Liabilities
- **Worker Credential Blast Radius**: Compromise of a worker host exposes the shared worker credential, enabling the attacker to claim tasks or submit results until the token is rotated.
- **No Untrusted Activity Execution**: Operators cannot safely execute untrusted third-party code on NexusFlow V1 workers without deploying external process or container sandboxes.
- **Manual Credential Rotation**: Rotating credentials requires updating configuration files or environment variables and applying configuration updates (potentially requiring a process restart in V1).

---

## 14. Failure Modes

### 14.1 Compromised Worker Credential
- *Threat*: An attacker extracts the worker Bearer token from a compromised worker node.
- *Impact*: The attacker can connect to the control plane, register sessions, claim tasks matching advertised capabilities, and submit fake results.
- *Mitigation*: The attacker cannot access the public API, cannot read raw database tables, and cannot bypass OCC revision guards or attempt lifecycle rules. Immediate credential rotation revokes access.

### 14.2 Stolen `WorkerSessionId` Without Credential
- *Threat*: An eavesdropper observes a `WorkerSessionId` from network traffic or logs.
- *Impact*: Zero direct compromise. Worker protocol endpoints reject requests lacking the valid worker Bearer credential during initial authentication.

### 14.3 Malicious YAML Definition Bomb
- *Threat*: An attacker registers a workflow definition containing recursive aliases or Billion Laughs expansion payloads.
- *Impact*: Control-plane memory exhaustion or CPU lockup.
- *Mitigation*: YAML parser runs in safe mode with bounded document byte sizes and structural complexity limits enforced before semantic validation.

### 14.4 Database Network Exposure
- *Threat*: PostgreSQL port is accidentally exposed to the public Internet.
- *Impact*: Vulnerable to external brute-force attacks or database exploits.
- *Mitigation*: Reference deployment binds PostgreSQL strictly to internal isolated networks; host port binding is disabled in production.

---

## 15. Debugging

Security failures must produce clear diagnostic telemetry without leaking sensitive credentials:
- **Authentication Rejection**: Emits a structured security log containing event type (`auth_failure`), client IP (if available), requested path, and failure reason (`missing_token`, `invalid_digest`, `domain_mismatch`). The raw token is **never** logged.
- **Authorization Denial**: Emits an event (`authorization_denied`) logging principal ID, requested operation, and missing permission.
- **Mismatched Attempt Authority**: Logged under the worker coordination category with `AttemptId`, `WorkerSessionId`, expected state, and actual state.
- **Diagnostic Fingerprinting**: If identifying a configured credential is required in logs, an optional, non-reversible truncated digest prefix (e.g., `SHA-256(token)[:8]`) may be referenced, but default configurations avoid logging token identifiers entirely.

---

## 16. Testing

Per [ADR-021](docs/architecture/adr-021-testing-strategy.md), security verification is incorporated directly into the test suite:

```
tests/
├── contract/
│   ├── test_public_api_auth.py   # Missing/invalid/valid tokens, permission checks
│   ├── test_worker_auth.py       # Worker token enforcement, public token rejection
│   └── test_error_sanitization.py# Verify no stack traces or SQL leak to API
├── integration/
│   ├── test_db_privileges.py     # Verify runtime DB role cannot execute DDL
│   └── test_telemetry_redaction.py# Assert headers & passwords absent from logs/traces
├── domain/
│   ├── test_yaml_security.py     # Safe mode parser, alias limits, tag rejection
│   └── test_payload_bounds.py    # Request body size enforcement
└── architecture/
    └── test_import_boundaries.py # Ensure domain code has zero security framework imports
```

- **Synthetic Test Credentials**: All test fixtures use synthetic credentials (e.g., `"test-public-token-secret-value-12345"`). Real deployment secrets are never committed to fixtures or code.
- **Automated CI Security Checks**:
  - `gitleaks`: Scans commits to detect accidentally committed tokens or private keys.
  - `pip-audit`: Scans `uv.lock` dependencies against known vulnerability advisory databases.
  - `bandit`: Executes periodic static analysis for common Python security defects.

---

## 17. Operational Considerations

- **Secret Injection**: In production reference deployments, secrets (`DATABASE_URL`, public API tokens, worker tokens) are injected via environment variables or mounted container secret files. Plaintext secrets are never committed to version control or baked into container images.
- **Zero-Downtime Rotation**: The control plane verifier supports configuring multiple valid credentials simultaneously during a transition window, allowing clients and workers to migrate to new tokens before the old credential is removed.
- **Local Developer Experience**: Local development runs with authentication enabled using explicit, pre-configured development tokens. Plaintext HTTP transport is permitted strictly on `localhost` or isolated local Docker Compose bridge networks.
- **Health & Metrics Endpoints**:
  - Health/readiness endpoints return minimal status (`UP`/`DOWN`) and are unauthenticated; they never leak database exception strings or topology details.
  - Prometheus `/metrics` endpoints should reside on an internal network path and be restricted from public Internet access.

---

## 18. Maintenance

- **Credential Lifecycle**: Operators must establish operational procedures for periodic rotation of public API and worker Bearer tokens. Revocation becomes effective once updated configuration is applied to the control plane (which may require a process restart in V1).
- **Dependency Patching**: The `pip-audit` CI check runs on pull requests and dependency updates; vulnerable packages must be patched or mitigated promptly.
- **Static Analysis Review**: Periodic Bandit reports must be reviewed to ensure new adapter code does not introduce unsafe shell execution, dynamic SQL concatenation, or unparameterized queries.

---

## 19. Future Evolution

- **ADR-024 (Workflow Versioning)**: Security checks will enforce that workflow definition updates verify write permissions against existing definitions.
- **ADR-025 (High Availability & Clustering)**: Multi-node control-plane deployments will require shared token configuration or integration with distributed secret stores.
- **ADR-026 (Client & Worker SDKs)**: SDKs will provide native support for Bearer token injection, automatic TLS certificate verification, and retry handling on authentication failures.
- **Post-V1 Enterprise Hardening (V2+)**:
  - *OAuth2 / OIDC*: External identity federation for public API access.
  - *Worker mTLS / SPIFFE*: Cryptographic per-worker process identity and hardware-backed attestation.
  - *Multi-Tenancy*: `TenantId` namespace segregation and tenant-scoped authorization.
  - *Task Sandboxing*: Container-per-task or microVM execution (e.g., Firecracker, gVisor) for executing mutually untrusted activity code.

---

## 20. Rejected Alternatives

- **Rejecting OAuth2 / OIDC for V1**: Imposes substantial operational overhead (identity provider management, token introspection, complex mock harnesses in tests) that is disproportionate for a single-developer V1 engine.
- **Rejecting Mandatory mTLS for Worker Protocol in V1**: Managing x509 PKI, certificate issuance, renewal lifecycles, and local developer certificate trust stores introduces severe friction without improving core orchestration correctness.
- **Rejecting Password Hashing (Argon2/bcrypt) for Machine Tokens**: High-entropy machine secrets are not vulnerable to dictionary attacks; fast SHA-256 digest comparison provides appropriate security without introducing artificial CPU bottlenecks.
- **Rejecting Process Sandboxing Guarantees in V1**: Thread pools cannot isolate memory or operating system environments. Promising security sandboxing for in-process activity code would be dishonest and dangerous.
- **Rejecting Ambient Cookie Authentication**: Eliminates CSRF attack vectors entirely by relying strictly on explicit `Authorization: Bearer` headers.

---

## 21. Decision Evolution

- **Initial Concept**: Traditional web application security relying on session cookies or standard JWT tokens.
- **Architectural Calibration**: Recognized that NexusFlow is an asynchronous distributed machine-to-machine orchestrator. Converted to distinct, high-entropy Bearer credential domains verified via constant-time SHA-256 digest comparison.
- **Separation of Authority**: Established the vital distinction between security authentication (caller domain membership) and orchestration authority (`WorkerSessionId`, `AttemptId`, OCC revision), preventing identity spoofing and stale-attempt races.
- **Trust Model Realism**: Explicitly documented that shared worker tokens do not provide per-process cryptographic identity, and that worker activity code runs under a trusted-code assumption.

---

## 22. Common Misconceptions

- *Misconception*: "`WorkerSessionId` is a secret token that authenticates the worker."  
  *Correction*: `WorkerSessionId` is an ephemeral runtime coordination identifier. Authentication occurs exclusively via the worker Bearer token in the `Authorization` header.
- *Misconception*: "Private Docker networks mean we do not need TLS or authentication."  
  *Correction*: Internal networks do not constitute an absolute security boundary. Defense in depth requires authentication and TLS across all untrusted or exposed boundaries.
- *Misconception*: "Executing activity handlers in Python threads provides security isolation."  
  *Correction*: Threads share the same address space, file descriptors, and environment variables. Activity code is completely unisolated from the worker process.
- *Misconception*: "Execution History (ADR-014) is a security audit log."  
  *Correction*: Execution History records semantic orchestration events (`TaskExecutionSucceeded`). Security events (authentication failures, invalid tokens) are logged separately to structured application diagnostic logs.

---

## 23. Open Questions

- *None*. The security architecture establishes a complete, robust, and minimal framework fully aligned with ADR-001 through ADR-021.

---

## 24. Interview Discussion

In systems engineering interviews, NexusFlow's security architecture demonstrates sophisticated defense-in-depth design for distributed systems:
- It highlights the critical separation between **cryptographic authentication** (who is calling) and **orchestration authority** (whether the authenticated caller holds valid execution rights on an attempt).
- It illustrates pragmatic machine-to-machine security: using 256-bit entropy machine tokens verified via constant-time SHA-256 digest comparison rather than misapplying human password hashing (bcrypt) or overengineering with OAuth2/OIDC for internal services.
- It demonstrates intellectual honesty in distributed systems: explicitly acknowledging that thread-pool workers do not sandbox untrusted code and documenting the exact blast radius of shared worker credentials rather than making false claims of "total security."

---

## 25. References

- [ADR-008: Worker Coordination & Liveness Model](docs/architecture/adr-008-worker-coordination-and-liveness.md)
- [ADR-010: Workflow Data Flow & Parameter Passing](docs/architecture/adr-010-workflow-data-flow-and-parameter-passing.md)
- [ADR-011: State Persistence Strategy](docs/architecture/adr-011-state-persistence.md)
- [ADR-013: Consistency & Concurrency Strategy](docs/architecture/adr-013-consistency-and-concurrency.md)
- [ADR-014: Execution History & Audit Model](docs/architecture/adr-014-execution-history-and-audit-model.md)
- [ADR-015: External API Architecture](docs/architecture/adr-015-external-api-architecture.md)
- [ADR-016: Observability Architecture](docs/architecture/adr-016-observability.md)
- [ADR-018: Error Handling Philosophy](docs/architecture/adr-018-error-handling-philosophy.md)
- [ADR-019: Project Modularity & Service Boundaries](docs/architecture/adr-019-project-and-service-boundaries.md)
- [ADR-020: Technology Selection Strategy](docs/architecture/adr-020-technology-selection.md)
- [ADR-021: Testing Strategy](docs/architecture/adr-021-testing-strategy.md)
- NIST SP 800-63B: Digital Identity Guidelines
- OWASP API Security Top 10

---

## 26. Traceability

### 26.1 Traceability to Architecture & Requirements

| Requirement / Invariant | Security Mechanism | Enforcement Layer | Verification Reference |
| :--- | :--- | :--- | :--- |
| **REQ-SEC-001** | Distinct public & worker Bearer credentials | Ingress Adapter / Middleware | `tests/contract/test_public_api_auth.py`, `test_worker_auth.py` |
| **REQ-SEC-002** | Mandatory TLS on untrusted boundaries | Reverse Proxy / Uvicorn TLS | Deployment & Docker Compose Integration Tests |
| **REQ-SEC-003** | Constant-time SHA-256 digest comparison | Security Ingress Adapter | Unit tests with timing-attack mitigation checks |
| **REQ-SEC-004** | Exclusive control-plane DB access; zero worker DB connectivity | Network Topology & Config | Architecture boundary & container network tests |
| **REQ-SEC-005** | Safe YAML parser mode, bounded payloads | Definition Ingestion Adapter | `tests/domain/test_yaml_security.py`, `test_payload_bounds.py` |
| **REQ-SEC-006** | Redaction of headers, URLs, and secrets | Telemetry Formatters | `tests/integration/test_telemetry_redaction.py` |
| **REQ-SEC-007** | Fail-closed on authentication/validation error | Ingress Adapter / Use-Cases | `tests/contract/test_public_api_auth.py` |
| **REQ-SEC-008** | Authentication decoupled from `(AttemptId, WorkerSessionId)` | Worker Protocol Adapter | `tests/contract/test_worker_auth.py` |
| **ADR-013 / 014** | Atomic state + history commit; OCC revision guards | PostgreSQL Persistence | `tests/integration/test_concurrency.py`, `test_persistence.py` |
| **ADR-018** | Normalized error envelopes; no leaked SQL/stack traces | Public API Error Handler | `tests/contract/test_error_sanitization.py` |

---

## 27. Decision Validation Checklist

- [x] **Distinct Credential Domains**: Public API and worker credentials are non-interchangeable.
- [x] **Constant-Time Verification**: Uses SHA-256 and `hmac.compare_digest`.
- [x] **Separation of Concerns**: Security authentication explicitly decoupled from `(AttemptId, WorkerSessionId)` orchestration authority.
- [x] **Worker Security Domain Clarified**: Shared worker token blast radius documented; does not claim per-process cryptographic identity.
- [x] **Transport Security Mandatory**: TLS required for untrusted/external networks; plaintext permitted strictly in isolated local development.
- [x] **Database Isolation**: Workers have zero database credentials and zero direct connectivity.
- [x] **Safe Parsing Enforced**: YAML safe mode and JSON structural size bounds prevent memory exhaustion and code execution.
- [x] **Redaction Enforced**: Authorization headers, passwords, and sensitive payloads excluded from logs, traces, and metrics.
- [x] **Trusted Activity Code Model**: Documented that thread-pool workers provide zero security sandboxing for untrusted activity code.
- [x] **Fail-Closed Operations**: Unauthenticated requests fail immediately without state or history mutation.
- [x] **Fidelity to Prior ADRs**: Preserves all state machines, OCC revisions, and coordination protocols without introducing unapproved states or mechanisms.
