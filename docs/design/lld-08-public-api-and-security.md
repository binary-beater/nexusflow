# NexusFlow V1 — LLD-08: Public API & Security

**Document Status:** Architecture-Ready / Approved as LLD-09 Input  
**Authoritative References:** ADR-001 (Intermediate Workflow Specification), ADR-002 (Definition Parsing), ADR-003 (Canonical Graph Representation), ADR-004 (Definition Validation), ADR-005 (Scheduler), ADR-006 (Workflow State Machine), ADR-007 (Task Lifecycle & Attempt Model), ADR-008 (Worker Coordination & Liveness), ADR-009 (Task Routing), ADR-010 (Workflow Data Flow), ADR-011 (State Persistence), ADR-012 (Recovery), ADR-013 (Consistency & Concurrency), ADR-014 (History), ADR-015 (Public Management API), ADR-016 (Observability), ADR-017 (Graceful Shutdown), ADR-018 (Error Handling), ADR-019 (Project / Service Boundaries), ADR-020 (Technology Selection), ADR-021 (Testing Strategy), ADR-022 (Security Architecture), ADR-023 (Configuration), NexusFlow V1 HLD, LLD-01 (Domain Model & Module Contracts), LLD-02 (PostgreSQL Schema & Persistence Transactions), LLD-03 (Definition Ingestion & Validation Pipeline), LLD-04 (Scheduling, Routing & Ownership), LLD-05 (Worker Protocol & Worker Runtime), LLD-06 (Execution Results, Retries, Timeouts & Cancellation), LLD-07 (Recovery & Reconciliation).  
**Downstream Dependents:** LLD-09 (Observability, Configuration & Runtime Lifecycle).

---

## 1. Primary Objective & Architectural Scope

### 1.1 Objective Statement
This document defines the complete implementation-level design for how external clients interact with NexusFlow V1 over HTTP/JSON, and how those requests are safely transported, authenticated, authorized, validated, translated into application use cases, mapped to responses, and protected against malicious or malformed input.

The HTTP layer is strictly an **ingress adapter**. It possesses **zero orchestration semantics** and contains no domain business logic. It translates external HTTP requests into typed domain commands, invokes application ports, and serializes domain/persistence outcomes into stable HTTP response contracts and error envelopes.

```text
HTTP Request
    │
    ▼
Transport / Size / Content-Type Guards (Raw Bytes & Header Checks)
    │
    ▼
Public Domain Bearer Authentication (Constant-Time Digest Check)
    │
    ▼
Authorization Guard (SecurityContext.has_permission)
    │
    ▼
Process Admission Guard (RecoveryGate.allows_new_work / allows_existing_settlement)
    │
    ▼
Pydantic v2 Request DTO Parsing & Boundary Validation
    │
    ▼
Application Port / Use Case Invocation (Typed Application Commands)
    │
    ▼
PostgreSQL Relational / OCC Outcome (CommitOutcome / RegistrationOutcome)
    │
    ▼
Stable Response DTO Serialization / Standard Error Envelope (JSON)
```

---

## 2. V1 API Boundaries & URL Namespaces

### 2.1 Frozen Public API Prefix: `/v1`
In strict compliance with ADR-015:
- All public management endpoints are rooted beneath the frozen prefix:
  ```text
  /v1
  ```
- There is no `/api/v1` namespace or secondary public URL root.

### 2.2 Internal Worker Boundary Separation
- Internal worker coordination endpoints reside beneath:
  ```text
  /internal/v1/worker/...
  ```
- These endpoints are owned by LLD-05 and LLD-06. LLD-08 formalizes their security domain boundary and credential separation (Section 11 & Section 17) without modifying their wire protocol or internal semantics.

### 2.3 System Health & Diagnostics
- Unauthenticated liveness and readiness diagnostic endpoints reside at root level:
  ```text
  GET /healthz
  GET /readyz
  ```
- Metrics inspection (if enabled) resides at:
  ```text
  GET /metrics
  ```

---

## 3. Public Resources & Immutability Rules

The public API exposes five first-class domain resources:

```text
Definition  (/v1/definitions)
Execution   (/v1/executions)
Task        (/v1/executions/{execution_id}/tasks)
Attempt     (/v1/executions/{execution_id}/tasks/{task_execution_id}/attempts)
History     (/v1/executions/{execution_id}/history)
```

### 3.1 Prohibited Generic Lifecycle Mutations
NexusFlow V1 strictly prohibits generic state-mutation endpoints:
```http
PATCH /v1/executions/{id}
{
  "state": "SUCCEEDED"
}
```
Clients **never directly mutate** lifecycle states. State transitions occur strictly through explicit domain commands (`POST /v1/executions`, `POST /v1/executions/{execution_id}/cancel`) or internal worker/scheduler orchestration.

### 3.2 Definition Immutability
A registered definition is **semantically immutable**.
- There are no `PUT`, `PATCH`, or `DELETE` endpoints for definitions.
- Workflow versioning is deferred from V1.
- Changing workflow topology requires registering a new definition, producing a distinct `DefinitionId`.

---

## 4. Public Endpoint Inventory & Route Specification

| HTTP Method | Path | Auth Domain | Required Permission | Request Body | Success Status | Idempotency Header | Recovery Gate Policy |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `POST` | `/v1/definitions` | Public | `definitions:write` | `raw_yaml` (YAML / JSON) | `201 Created` | `Idempotency-Key` (Opt) | `allows_new_work` (Blocked) |
| `GET` | `/v1/definitions` | Public | `definitions:read` | None | `200 OK` | None | Read-only (Allowed) |
| `GET` | `/v1/definitions/{definition_id}` | Public | `definitions:read` | None | `200 OK` | None | Read-only (Allowed) |
| `POST` | `/v1/executions` | Public | `executions:start` | `CreateExecutionRequestDTO` | `201 Created` | `Idempotency-Key` (Opt) | `allows_new_work` (Blocked) |
| `GET` | `/v1/executions` | Public | `executions:read` | None | `200 OK` | None | Read-only (Allowed) |
| `GET` | `/v1/executions/{execution_id}` | Public | `executions:read` | None | `200 OK` | None | Read-only (Allowed) |
| `POST` | `/v1/executions/{execution_id}/cancel` | Public | `executions:cancel` | None | `200 OK` / `202 Accepted` | None | `allows_new_work` (Blocked) |
| `GET` | `/v1/executions/{execution_id}/tasks` | Public | `executions:read` | None | `200 OK` | None | Read-only (Allowed) |
| `GET` | `/v1/executions/{execution_id}/tasks/{task_execution_id}` | Public | `executions:read` | None | `200 OK` | None | Read-only (Allowed) |
| `GET` | `/v1/executions/{execution_id}/tasks/{task_execution_id}/attempts` | Public | `executions:read` | None | `200 OK` | None | Read-only (Allowed) |
| `GET` | `/v1/executions/{execution_id}/history` | Public | `executions:read` | None | `200 OK` | None | Read-only (Allowed) |

---

## 5. Definition Registration Pipeline & Semantic Idempotency

### 5.1 Registration Workflow & LLD-03 Ownership
Registration ingests the external YAML workflow representation via `POST /v1/definitions`. The HTTP adapter does not implement YAML parsing, normalization, or validation; it delegates completely to LLD-03:

```text
HTTP Client (raw YAML)
      │
      ▼
Transport Size Guard (<= 1 MB) & Content-Type Check
      │
      ▼
Bearer Auth & definitions:write Permission Check
      │
      ▼
Recovery Gate Check (allows_new_work == True)
      │
      ▼
LLD-03 DefinitionRegistrationUseCase
      ├── 1. Safe YAML Compose & Complexity Scan (ruamel.yaml)
      ├── 2. External DTO Parse (extra='forbid')
      ├── 3. Semantic Graph Validation (ADR-004, Cycle Detect)
      ├── 4. ValidatedWorkflowSpec Construction
      ├── 5. Deterministic Semantic RequestFingerprint (SHA-256 of canonical Validated IWS)
      └── 6. LLD-02 Persistence: commit_definition_registration
            └── ON CONFLICT DO NOTHING on idempotency_records
      │
      ▼
201 Created (DefinitionResponseDTO)
```

`201 Created` is returned **only after** the definition is durably written to PostgreSQL.

### 5.2 Semantic vs. Presentation-Only Idempotency Fingerprint
Definition registration idempotency request equivalence is based on the **semantic registration request / deterministic canonical serialization of the Validated IWS**, using the existing LLD-02 `RequestFingerprint` contract. It is **never** based on raw or reformatted YAML text.

#### Presentation-Only Invariance Rules:
The following syntactic presentation differences must produce the **exact same** `RequestFingerprint` when submitted with the same `Idempotency-Key`:
- Different whitespace or blank lines.
- Different indentation (where semantically equivalent).
- Different mapping key order in external YAML.
- Presence, absence, or modification of YAML comments.
- Different quoting styles (single, double, unquoted).

#### Semantic Difference Sensitivity:
Any alteration in actual workflow execution semantics produces a **different** `RequestFingerprint`:
- Adding, removing, or renaming tasks.
- Altering dependencies between tasks.
- Modifying literal input bindings or task output bindings.
- Changing `max_attempts` or activity routing type.
- Modifying workflow-level output bindings.

If a request arrives with the same `Idempotency-Key` but differing semantics, the transaction aborts and returns `409 Conflict` (`IDEMPOTENCY_CONFLICT`).

### 5.3 Content-Type Acceptance
`POST /v1/definitions` accepts:
- `application/yaml` or `text/yaml` (Raw YAML string body).
- `application/json` with DTO wrapper:
  ```json
  {
    "yaml_content": "spec_version: \"1.0\"\nworkflow: ..."
  }
  ```
Requests with unsupported `Content-Type` headers are immediately rejected with `415 Unsupported Media Type`.

---

## 6. Execution Start Contract & Lifecycle Visibility

### 6.1 Execution Creation Pipeline
Clients start workflow runs via `POST /v1/executions`:
```text
HTTP Client (JSON payload)
      │
      ▼
Transport Size Guard (<= 1 MB) & Content-Type: application/json
      │
      ▼
Bearer Auth & executions:start Permission Check
      │
      ▼
Recovery Gate Check (allows_new_work == True)
      │
      ▼
Pydantic DTO Validation (definition_id, workflow_input)
      │
      ▼
LLD-01 StartExecutionUseCase
      ├── 1. Verify definition exists in DB
      ├── 2. Validate input against freeze_json rules
      ├── 3. Compute canonical RequestFingerprint (SHA-256 of canonical JSON)
      └── 4. LLD-02 commit_workflow_creation (INITIALIZING, revision=1)
            └── ON CONFLICT DO NOTHING on idempotency_records
      │
      ▼
201 Created (ExecutionResponseDTO)
```

### 6.2 Asynchronous Execution Semantics (ADR-015)
- `201 Created` indicates **durable acceptance** of workflow creation into PostgreSQL.
- It does **not** indicate that workflow tasks have executed or that the workflow is complete.
- The returned `state` field will legitimately be `INITIALIZING` or `RUNNING`, depending on whether synchronous task population completed prior to HTTP response serialization.

---

## 7. Execution Cancellation Endpoint & State Behavior

### 7.1 Cancellation Command Route
Cancellation is an explicit lifecycle command:
```http
POST /v1/executions/{execution_id}/cancel
```
It accepts no request body and requires `executions:cancel` permission.

### 7.2 State-Dependent HTTP Responses (ADR-015 / LLD-06)
| Current Execution State in DB | Transition Result | HTTP Status Code | Response Body `state` | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `INITIALIZING` | `INITIALIZING → CANCELLING` | `202 Accepted` | `CANCELLING` | Transition committed; unstarted tasks cancelled. |
| `RUNNING` | `RUNNING → CANCELLING` | `202 Accepted` | `CANCELLING` | Transition committed; active attempts enter drain. |
| `CANCELLING` | Idempotent No-Op | `200 OK` | `CANCELLING` | Workflow is already draining cancellation. |
| `CANCELLED` | Idempotent No-Op | `200 OK` | `CANCELLED` | Workflow is already terminal cancelled. |
| `FAILING` | Conflict | `409 Conflict` | `FAILING` | Workflow is draining definitive task failure. |
| `FAILED` | Conflict | `409 Conflict` | `FAILED` | Terminal failed; cannot be cancelled. |
| `SUCCEEDED` | Conflict | `409 Conflict` | `SUCCEEDED` | Terminal succeeded; cannot be cancelled. |

The endpoint does **not** wait synchronously for the workflow to reach terminal `CANCELLED`. It returns immediately once the cancellation direction is durably committed to PostgreSQL via optimistic concurrency control (OCC).

---

## 8. API Idempotency Implementation

### 8.1 Idempotency Header & Request Identity
Clients provide an optional opaque token via the standard header:
```http
Idempotency-Key: <token>
```
- **Constraints:** String length between 1 and 256 characters.
- Supported on mutating creation endpoints:
  - `POST /v1/definitions`
  - `POST /v1/executions`

### 8.2 Canonical Request Fingerprinting
Under LLD-01, LLD-02, and LLD-03, semantic equivalence is validated using a cryptographic SHA-256 digest (`RequestFingerprint`):
- **Definition Registration Fingerprint:**
  Generated from the canonical serialization of the validated, immutable IWS (`serialize_validated_spec(spec)`):
  ```python
  canonical_bytes = json.dumps(
      serialize_validated_spec(spec),
      sort_keys=True,
      separators=(",", ":"),
      ensure_ascii=False,
      allow_nan=False,
  ).encode("utf-8")
  RequestFingerprint = hashlib.sha256(canonical_bytes).hexdigest()
  ```
- **Execution Start Fingerprint:**
  Generated from canonical serialization of the execution start request:
  ```python
  canonical_bytes = json.dumps(
      {"definition_id": str(definition_id), "workflow_input": thaw_json(workflow_input)},
      sort_keys=True,
      separators=(",", ":"),
      ensure_ascii=False,
      allow_nan=False,
  ).encode("utf-8")
  RequestFingerprint = hashlib.sha256(canonical_bytes).hexdigest()
  ```

### 8.3 PostgreSQL Idempotency Transaction Semantics
Idempotency relies entirely on PostgreSQL 16 table `idempotency_records` (no Redis or distributed cache):

```sql
INSERT INTO idempotency_records (
    operation_type, idempotency_key, request_fingerprint, resource_id, created_at_utc
) VALUES (
    :operation_type, :idempotency_key, :fingerprint, :resource_id, :now_utc
)
ON CONFLICT (operation_type, idempotency_key) DO NOTHING;
```

1. **First Request:**
   - `ON CONFLICT DO NOTHING` inserts the record (`rowcount == 1`).
   - The primary resource (`WorkflowExecution` or `RegisteredDefinition`) is inserted within the same transaction.
   - Transaction commits $\to$ returns `201 Created`.
2. **Equivalent Request (Same Token, Same Fingerprint):**
   - Insert fails conflict (`rowcount == 0`).
   - Transaction reads existing row from `idempotency_records`.
   - Fingerprint matches $\to$ reads existing resource $\to$ returns original resource with `200 OK` or `201 Created`.
3. **Conflicting Request (Same Token, Different Fingerprint):**
   - Insert fails conflict (`rowcount == 0`).
   - Fingerprint does not match existing record $\to$ transaction aborts $\to$ returns `409 Conflict` (`IDEMPOTENCY_CONFLICT`).
4. **Concurrent Requests:**
   - PostgreSQL unique constraints on `(operation_type, idempotency_key)` serialize creators. Exactly one request wins insertion; the loser reads the winner's committed resource.
5. **Unknown Commit / Network Drop:**
   - If the client times out during commit, it retries with the same `Idempotency-Key`.
   - The server inspects `idempotency_records`: if committed, it returns the existing resource; if uncommitted, it safely executes creation.

---

## 9. Recovery Admission Integration & Read-Only Gating

LLD-08 strictly consumes the process admission policy established in LLD-07:

```python
class RecoveryGate(Protocol):
    def is_recovery_complete(self) -> bool: ...
    def allows_new_work(self) -> bool: ...
    def allows_existing_settlement(self) -> bool: ...
```

### 9.1 Admission Rules During Startup Recovery (`allows_new_work == False`)
1. **Public Mutation Endpoints Blocked:**
   - `POST /v1/definitions`
   - `POST /v1/executions`
   - `POST /v1/executions/{execution_id}/cancel`
   - **Behavior:** Rejected with `503 Service Unavailable` and error code `NOT_READY`.
2. **Public Read-Only Endpoints Admitted:**
   - `GET /v1/definitions/*`
   - `GET /v1/executions/*`
   - **Behavior:** Once the database connection and Alembic schema are verified (Step 2 of startup), read-only inspection is safe and admitted. Read availability does **not** imply readiness to orchestrate new work.
3. **System Diagnostic Endpoints:**
   - `GET /healthz` returns `200 OK` (liveness).
   - `GET /readyz` returns `503 Service Unavailable` until recovery convergence.
4. **Worker Settlement Boundary:**
   - `POST /internal/v1/worker/callback` for pre-restart `RUNNING` attempts is admitted via `allows_existing_settlement == True` (per LLD-07 Section 2).

---

## 10. Authentication Architecture & Credential Domains

In strict accordance with ADR-022:
- NexusFlow enforces **two strictly separated credential domains**:
  1. **Public API Credential Domain**
  2. **Worker API Credential Domain**

```text
[ Incoming Request: Authorization: Bearer <token> ]
                         │
                         ▼
        ┌─────────────────────────────────┐
        │ Which route is being accessed?  │
        └─────────────────────────────────┘
                 │               │
         /v1/*   ▼               ▼  /internal/v1/*
 ┌───────────────────────────┐  ┌───────────────────────────┐
 │ Public Authenticator      │  │ Worker Authenticator      │
 │  - Matches Public Secrets │  │  - Matches Worker Secret  │
 └───────────────────────────┘  └───────────────────────────┘
```

### 10.1 Domain Separation Rules
1. **Never Interchangeable:** A valid Worker credential used on a Public API endpoint is **rejected** (`401 Unauthorized`). A valid Public credential used on an internal Worker endpoint is **rejected** (`401 Unauthorized`).
2. **Identifiers Are Never Secrets:** `WorkerSessionId`, `AttemptId`, `DefinitionId`, and `WorkflowExecutionId` are public identifiers. They are never treated as credentials or used to infer authentication.
3. **Transport Mechanism:** High-entropy Bearer tokens supplied via the standard header:
   ```http
   Authorization: Bearer <token>
   ```
4. **No Complex Auth Machinery:** ADR-022 explicitly rejects OAuth servers, dynamic API key databases, cookies/sessions, mTLS user mapping, and HMAC request signing.

---

## 11. Credential Ingestion, Comparison & Security Context

### 11.1 Secret Ingestion & Storage
- Configured via environment variables (`NEXUSFLOW_PUBLIC_API_TOKENS`, `NEXUSFLOW_WORKER_DOMAIN_TOKEN`).
- Raw tokens are **never stored in plaintext memory** after bootstrap.
- Upon startup, NexusFlow computes and retains only the cryptographic SHA-256 digests:
  ```python
  expected_digest = hashlib.sha256(raw_configured_token.encode("utf-8")).digest()
  ```

### 11.2 Constant-Time Comparison Path
To defeat timing attacks, supplied tokens are digested and compared using `hmac.compare_digest`:
```python
supplied_digest = hashlib.sha256(supplied_token.encode("utf-8")).digest()
is_valid = hmac.compare_digest(expected_digest, supplied_digest)
```

### 11.3 Sanitized Application SecurityContext
Application code never receives raw tokens. Upon successful authentication, FastAPI constructs the frozen LLD-01 `SecurityContext`:
```python
@dataclass(frozen=True, slots=True)
class SecurityContext:
    principal_id: str
    principal_type: PrincipalType
    permissions: frozenset[PublicPermission]

    def has_permission(self, permission: PublicPermission) -> bool:
        return permission in self.permissions
```

---

## 12. Public Authorization Matrix (ADR-022)

NexusFlow uses explicit permissions without complex dynamic RBAC trees:
- `definitions:read`
- `definitions:write`
- `executions:read`
- `executions:start`
- `executions:cancel`

### 12.1 Endpoint Permission Enforcement
```python
def require_permission(required: PublicPermission):
    async def dependency(
        ctx: SecurityContext = Depends(get_public_security_context),
    ) -> SecurityContext:
        if not ctx.has_permission(required):
            raise ApiHttpException(
                status_code=403,
                code="FORBIDDEN",
                message="Principal lacks required permission for this operation.",
            )
        return ctx

    return dependency
```

### 12.2 Authentication vs. Authorization Failure
- **Authentication Failure (`401 Unauthorized`):** Missing, malformed, or invalid Bearer token. Returns standard header:
  ```http
  WWW-Authenticate: Bearer error="invalid_token"
  ```
- **Authorization Failure (`403 Forbidden`):** Valid Bearer token authenticated, but principal's `SecurityContext` lacks the specific endpoint permission.

---

## 13. TLS & Network Boundaries

In accordance with ADR-022:
- **Production Deployments:** TLS 1.3 (or TLS 1.2) is **strictly mandatory** for all external public and worker traffic.
- **TLS Termination:** The NexusFlow Python application does not implement in-process TLS termination. Termination occurs at an external reverse proxy, ingress controller, or load balancer (e.g., Envoy, Traefik, NGINX).
- **Local Development:** Unencrypted HTTP is permitted only in isolated local development environments (e.g., Docker Compose on loopback interfaces).

---

## 14. Request & Response DTO Specification (Pydantic v2)

DTOs reside strictly at system boundaries (`src/nexusflow/interfaces/http/dto.py`). Core domain entities never inherit from Pydantic `BaseModel`.

### 14.1 Request DTOs
```python
from pydantic import BaseModel, ConfigDict, Field
from uuid import UUID
from typing import Any


class RegisterDefinitionJsonDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    yaml_content: str = Field(..., min_length=1, max_length=1_000_000)


class CreateExecutionRequestDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    definition_id: UUID
    workflow_input: Any = Field(default=None)
```

### 14.2 Output Presence Representation (ADR-010)
To avoid ambiguity under JSON serialization where SQL `NULL` and JSON `'null'` must remain distinct:
```json
// Task / Workflow Still Running (Output Uncommitted):
{
  "has_output": false
}

// Task / Workflow Succeeded with Explicit JSON null:
{
  "has_output": true,
  "output": null
}

// Task / Workflow Succeeded with JSON Document:
{
  "has_output": true,
  "output": { "result": "computed_value" }
}
```

### 14.3 Response DTOs
In strict accordance with ADR-014 and LLD-02:
- `history_entries` possesses **no sequence counter** (`sequence_number` does not exist in schema or public API).
- Physical history retrieval uses `(occurred_at_utc, history_id)`.

```python
from datetime import datetime
from pydantic import BaseModel, ConfigDict
from uuid import UUID
from typing import Any


class DefinitionResponseDTO(BaseModel):
    model_config = ConfigDict(frozen=True)
    definition_id: UUID
    workflow_name: str
    spec_version: str
    created_at_utc: datetime


class ExecutionResponseDTO(BaseModel):
    model_config = ConfigDict(frozen=True)
    workflow_execution_id: UUID
    definition_id: UUID
    state: str
    has_output: bool
    output: Any | None = None
    created_at_utc: datetime
    updated_at_utc: datetime


class FailureCauseDTO(BaseModel):
    model_config = ConfigDict(frozen=True)
    category: str
    code: str
    message: str
    details: dict[str, Any] | None = None


class TaskExecutionResponseDTO(BaseModel):
    model_config = ConfigDict(frozen=True)
    task_execution_id: UUID
    workflow_execution_id: UUID
    task_definition_id: str
    state: str
    current_attempt_ordinal: int
    has_output: bool
    output: Any | None = None
    failure_cause: FailureCauseDTO | None = None
    created_at_utc: datetime
    updated_at_utc: datetime


class ExecutionAttemptResponseDTO(BaseModel):
    model_config = ConfigDict(frozen=True)
    attempt_id: UUID
    task_execution_id: UUID
    attempt_ordinal: int
    state: str
    worker_session_id: UUID
    failure_cause: FailureCauseDTO | None = None
    start_deadline_utc: datetime
    execution_timeout_utc: datetime | None = None
    created_at_utc: datetime
    updated_at_utc: datetime


class HistoryEntryResponseDTO(BaseModel):
    """
    Public representation of an immutable audit trail entry.
    Contains no synthetic monotonic sequence counter (ADR-014).
    """

    model_config = ConfigDict(frozen=True)
    history_id: UUID
    workflow_execution_id: UUID
    task_execution_id: UUID | None = None
    attempt_id: UUID | None = None
    event_category: str
    occurred_at_utc: datetime
    details: dict[str, Any]
```

---

## 15. Standard Error Envelope & Taxonomy Mapping

### 15.1 Stable Error Envelope Schema (ADR-018)
Every 4xx and 5xx response returned by NexusFlow adheres to a single JSON envelope:
```json
{
  "error": {
    "code": "EXECUTION_NOT_FOUND",
    "message": "Workflow execution was not found.",
    "details": {},
    "request_id": "req-98fbc18d-e43b-483a-8742-2d88c42a201b"
  }
}
```

### 15.2 Information Sanitization Guardrails
- **No Stack Traces:** Python tracebacks are strictly excluded from all HTTP responses.
- **No SQL Statements:** PostgreSQL errors, constraint names, and query fragments are never exposed.
- **No Secret Leakage:** Configuration values and Bearer tokens are completely scrubbed.
- **Bounded Details:** The `details` mapping is capped at 32 KB.

### 15.3 Comprehensive Application Outcome Mapping
| Application Outcome | ADR-018 Category | HTTP Status Code | Stable Error Code | Retry Guidance |
| :--- | :--- | :--- | :--- | :--- |
| Malformed JSON syntax | `CLIENT_INPUT` | `400 Bad Request` | `MALFORMED_JSON` | Fix JSON syntax; do not retry. |
| Missing required DTO fields | `CLIENT_INPUT` | `422 Unprocessable Content`| `VALIDATION_ERROR` | Fix payload schema; do not retry. |
| Payload exceeds size limit | `CLIENT_INPUT` | `413 Payload Too Large` | `PAYLOAD_TOO_LARGE` | Reduce request size. |
| Unsupported Media Type | `CLIENT_INPUT` | `415 Unsupported Media Type`| `UNSUPPORTED_MEDIA_TYPE`| Set supported Content-Type header. |
| YAML structural syntax error | `VALIDATION` | `400 Bad Request` | `MALFORMED_YAML` | Fix YAML formatting. |
| YAML exceeds node/depth limit| `VALIDATION` | `422 Unprocessable Content`| `YAML_STRUCTURE_INVALID` | Simplify YAML structure. |
| Semantic definition validation error| `VALIDATION` | `422 Unprocessable Content`| `DEFINITION_VALIDATION_FAILED`| Fix workflow graph semantics. |
| Missing or invalid Bearer auth | `SECURITY` | `401 Unauthorized` | `UNAUTHORIZED` | Provide valid Bearer token. |
| Insufficient endpoint permissions| `SECURITY` | `403 Forbidden` | `FORBIDDEN` | Request elevated permissions. |
| Definition ID not found in DB | `DOMAIN_EXECUTION` | `404 Not Found` | `DEFINITION_NOT_FOUND` | Verify definition ID. |
| Execution ID not found in DB | `DOMAIN_EXECUTION` | `404 Not Found` | `EXECUTION_NOT_FOUND` | Verify execution ID. |
| Task ID not found in DB | `DOMAIN_EXECUTION` | `404 Not Found` | `TASK_NOT_FOUND` | Verify task ID. |
| Attempt ID not found in DB | `DOMAIN_EXECUTION` | `404 Not Found` | `ATTEMPT_NOT_FOUND` | Verify attempt ID. |
| Conflicting Idempotency-Key reuse| `DOMAIN_CONFLICT` | `409 Conflict` | `IDEMPOTENCY_CONFLICT` | Generate new Idempotency-Key. |
| Illegal cancellation attempt | `DOMAIN_CONFLICT` | `409 Conflict` | `EXECUTION_NOT_CANCELLABLE`| Execution in non-cancellable state. |
| Process in startup recovery | `SYSTEM_TRANSIENT` | `503 Service Unavailable` | `NOT_READY` | Retry after recovery convergence. |
| Database connection pool exhausted| `SYSTEM_TRANSIENT` | `503 Service Unavailable` | `SERVICE_UNAVAILABLE` | Retry with exponential backoff. |
| PostgreSQL commit timeout | `UNKNOWN_OUTCOME` | `500 Internal Server Error`| `COMMIT_OUTCOME_UNKNOWN` | Reread resource before retrying. |
| Unhandled internal exception | `SYSTEM_PERMANENT` | `500 Internal Server Error`| `INTERNAL_SERVER_ERROR` | Contact support; do not retry blindly. |

---

## 16. Input Safety & Transport Guards

### 16.1 Transport Payload Size Limits
Enforced by ASGI middleware prior to body deserialization:
- **Maximum Request Body Size:** `1,048,576 bytes` (1 MB).
- Requests exceeding 1 MB are terminated immediately with `413 Payload Too Large` without consuming further server memory.

### 16.2 Safe YAML Parsing Protection (ADR-002 / LLD-03)
When processing YAML definitions:
- **Parser Engine:** `ruamel.yaml.YAML(typ="safe", pure=True)`.
- **Node Count Limit:** Maximum 1,000 composed nodes.
- **Nesting Depth Limit:** Maximum depth of 16 levels.
- **Alias / Anchor Bomb Rejection:** YAML anchors (`&`) and aliases (`*`) are prohibited; presence triggers immediate rejection.

---

## 17. Bounded Keyset Pagination Architecture & History Ordering

All resource listing endpoints enforce bounded keyset pagination. Offset-based (`LIMIT ... OFFSET ...`) pagination is strictly prohibited to guarantee constant-time database scans and eliminate page-drift anomalies.

### 17.1 Query Parameters
- `limit` (int, default=50, max=100): Number of records to return.
- `cursor` (str, optional): Base64url-encoded opaque pagination cursor.

### 17.2 Opaque Cursor Codec
Cursors encode monotonic database sort keys:
```python
import base64
import json


class CursorCodec:
    @staticmethod
    def encode(cursor_data: dict[str, Any]) -> str:
        json_str = json.dumps(cursor_data, separators=(",", ":"))
        return base64.urlsafe_b64encode(json_str.encode("utf-8")).decode("utf-8")

    @staticmethod
    def decode(cursor_str: str) -> dict[str, Any]:
        try:
            json_str = base64.urlsafe_b64decode(cursor_str.encode("utf-8")).decode("utf-8")
            return json.loads(json_str)
        except Exception as err:
            raise ApiHttpException(
                status_code=400,
                code="INVALID_CURSOR",
                message="Pagination cursor is invalid or corrupt.",
            )
```

### 17.3 Paginated Response Envelope
```python
from typing import Generic, TypeVar
from pydantic import BaseModel

T = TypeVar("T")


class PaginatedListResponseDTO(BaseModel, Generic[T]):
    items: list[T]
    next_cursor: str | None = None
```

### 17.4 Resource Keyset Pagination Definitions
- **Executions Listing:** Ordered by `created_at_utc ASC, workflow_execution_id ASC`.
  - Filters supported: `state` (WorkflowState), `definition_id` (UUID).
- **Tasks Listing:** Ordered by `created_at_utc ASC, task_execution_id ASC`.
- **Attempts Listing:** Ordered by `attempt_ordinal ASC, attempt_id ASC`.
- **History Listing (ADR-014, LLD-02):** Ordered by `occurred_at_utc ASC, history_id ASC`.

### 17.5 History Ordering Semantics vs. Causal Ordering (ADR-014)
In strict accordance with ADR-014:
1. **Physical Presentation Order Only:** Keyset sorting on `(occurred_at_utc, history_id)` provides a deterministic physical cursor pagination order. It **does not establish an authoritative total semantic order** among concurrent independent task events.
2. **No Engine-Wide Monotonic Sequence:** There is no `sequence_number`, global counter, or per-workflow history sequence. NexusFlow does not serialize unrelated events through a distributed coordinator.
3. **Causal Ordering Authority:** Causal ordering for a specific task or attempt is derived strictly from durable entity lifecycle state, revisions, and monotonic attempt ordinals (`attempt_ordinal >= 1`).
4. **Non-Authoritative Audit Trail:** History is strictly an immutable audit trail. Clients must never attempt to replay history events to reconstruct current workflow state. Current relational database state in PostgreSQL is the sole orchestration authority.

#### History Keyset Query Contract (LLD-02):
```sql
SELECT
    history_id,
    workflow_execution_id,
    task_execution_id,
    attempt_id,
    event_category,
    event_payload,
    occurred_at_utc
FROM history_entries
WHERE workflow_execution_id = :workflow_execution_id
  AND (
      occurred_at_utc,
      history_id
  ) > (
      :cursor_occurred_at,
      :cursor_history_id
  )
ORDER BY
    occurred_at_utc ASC,
    history_id ASC
LIMIT :limit;
```

---

## 18. Request Correlation & Observability Boundary

### 18.1 Request Correlation Middleware
Every incoming HTTP request is assigned a unique UUIDv4 `request_id`:
- Extracted from `X-Request-ID` header if valid UUID syntax; otherwise freshly generated.
- Injected into ASGI `request.state.request_id` and Python `contextvars` for structured logging.
- Echoed back on all HTTP responses via the header:
  ```http
  X-Request-ID: req-98fbc18d-e43b-483a-8742-2d88c42a201b
  ```

### 18.2 Security-Safe Structured Logging
- **Permitted Fields:** `http_method`, `route_path`, `status_code`, `duration_ms`, `request_id`, `principal_id`, `client_ip`.
- **Strictly Redacted / Prohibited:**
  - `Authorization` headers.
  - Raw YAML definition payloads.
  - `workflow_input` and `workflow_output` payloads.
  - Worker Bearer tokens.
  - Exception tracebacks.

---

## 19. Package & Module Architecture

Consistent with LLD-01 and ADR-019:

```text
src/nexusflow/
    interfaces/
        http/
            app.py                  # FastAPI application factory & router mount (/v1)
            dependencies.py         # Auth, RecoveryGate, & port injection
            errors.py               # Centralized exception handlers & error envelope
            middleware.py           # Size guard, request correlation, & logging
            dto.py                  # Pydantic v2 boundary DTOs
            security.py             # PublicAuthenticator & constant-time digest check
            cursor.py               # Keyset CursorCodec

            public/
                definitions.py      # /v1/definitions routes
                executions.py       # /v1/executions routes
                tasks.py            # /v1/executions/{id}/tasks routes
                attempts.py         # /v1/executions/{id}/tasks/{id}/attempts routes
                history.py          # /v1/executions/{id}/history routes

            system/
                health.py           # /healthz and /readyz routes
```

---

## 20. Concrete Code Contracts & Interfaces

### 20.1 Public Authenticator
```python
import hashlib
import hmac
from typing import Mapping
from fastapi import Request
from nexusflow.domain.security import SecurityContext, PrincipalType, PublicPermission
from nexusflow.interfaces.http.errors import ApiHttpException


class PublicAuthenticator:
    def __init__(self, configured_tokens: Mapping[str, frozenset[PublicPermission]]):
        # Precompute SHA-256 digests for configured secrets
        self._token_digests: dict[bytes, tuple[str, frozenset[PublicPermission]]] = {
            hashlib.sha256(token.encode("utf-8")).digest(): (f"principal-{idx}", perms)
            for idx, (token, perms) in enumerate(configured_tokens.items())
        }

    def authenticate(self, request: Request) -> SecurityContext:
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            raise ApiHttpException(
                status_code=401,
                code="UNAUTHORIZED",
                message="Missing or invalid Bearer authorization header.",
            )

        supplied_token = auth_header[7:].strip()
        supplied_digest = hashlib.sha256(supplied_token.encode("utf-8")).digest()

        for expected_digest, (principal_id, perms) in self._token_digests.items():
            if hmac.compare_digest(expected_digest, supplied_digest):
                return SecurityContext(
                    principal_id=principal_id,
                    principal_type=PrincipalType.PUBLIC_CLIENT,
                    permissions=perms,
                )

        raise ApiHttpException(
            status_code=401, code="UNAUTHORIZED", message="Invalid Bearer credential."
        )
```

### 20.2 FastAPI Recovery Admission Guard
```python
from fastapi import Depends
from nexusflow.interfaces.http.errors import ApiHttpException
from nexusflow.ports.recovery import RecoveryGate


def require_new_work_admitted(gate: RecoveryGate = Depends(get_recovery_gate)) -> None:
    if not gate.allows_new_work():
        raise ApiHttpException(
            status_code=503,
            code="NOT_READY",
            message="Control plane is reconciling startup state. New work mutations blocked.",
        )
```

---

## 21. Sequence Diagrams

### 21.1 Definition Registration Flow
```text
Client                  FastAPI Router          Authenticator           RecoveryGate      DefinitionService (LLD-03)    PostgreSQL
  │                           │                       │                       │                       │                     │
  │ 1. POST /v1/definitions   │                       │                       │                       │                     │
  ├──────────────────────────>│                       │                       │                       │                     │
  │                           │ 2. Authenticate()     │                       │                       │                     │
  │                           ├──────────────────────>│                       │                       │                     │
  │                           │    SecurityContext    │                       │                       │                     │
  │                           │<──────────────────────┤                       │                       │                     │
  │                           │ 3. Check Gate         │                       │                       │                     │
  │                           ├──────────────────────────────────────────────>│                       │                     │
  │                           │    allows_new_work == True                    │                       │                     │
  │                           │<──────────────────────────────────────────────┤                       │                     │
  │                           │ 4. register_definition(raw_yaml, idem_key)    │                       │                     │
  │                           ├──────────────────────────────────────────────────────────────────────>│                     │
  │                           │                       │                       │                       │ 5. Parse & Validate │
  │                           │                       │                       │                       │    Canonical IWS    │
  │                           │                       │                       │                       │    Semantic SHA-256 │
  │                           │                       │                       │                       │ 6. commit_def_reg   │
  │                           │                       │                       │                       ├────────────────────>│
  │                           │                       │                       │                       │    COMMITTED (ID)   │
  │                           │                       │                       │                       │<────────────────────┤
  │                           │ 7. DefinitionRegisteredResult(id)             │                       │                     │
  │                           │<──────────────────────────────────────────────────────────────────────┤                     │
  │ 8. 201 Created (DTO)      │                       │                       │                       │                     │
  │<──────────────────────────┤                       │                       │                       │                     │
```

### 21.2 Concurrent Execution Start with Same Idempotency-Key
```text
Client A               Client B             FastAPI Endpoint               PostgreSQL (idempotency_records)
   │                      │                         │                                     │
   │ 1. POST /v1/execs    │                         │                                     │
   │    (Idem-Key: K1)    │                         │                                     │
   ├──────────────────────┼────────────────────────>│                                     │
   │                      │ 2. POST /v1/execs       │                                     │
   │                      │    (Idem-Key: K1)       │                                     │
   │                      ├────────────────────────>│                                     │
   │                      │                         │ 3. Client A Tx begins               │
   │                      │                         │    INSERT ON CONFLICT DO NOTHING    │
   │                      │                         ├────────────────────────────────────>│
   │                      │                         │    Rowcount == 1 (Winner)           │
   │                      │                         │    Insert WorkflowExecution         │
   │                      │                         │    Commit Transaction               │
   │                      │                         │<────────────────────────────────────┤
   │                      │                         │                                     │
   │                      │                         │ 4. Client B Tx begins               │
   │                      │                         │    INSERT ON CONFLICT DO NOTHING    │
   │                      │                         ├────────────────────────────────────>│
   │                      │                         │    Rowcount == 0 (Conflict)         │
   │                      │                         │    SELECT existing WHERE key=K1     │
   │                      │                         │    Fingerprint Matches!             │
   │                      │                         │<────────────────────────────────────┤
   │ 5. 201 Created       │                         │                                     │
   │    (Execution ID: E1)│                         │                                     │
   │<─────────────────────┼─────────────────────────┤                                     │
   │                      │ 6. 201 Created          │                                     │
   │                      │    (Execution ID: E1)   │                                     │
   │                      │<────────────────────────┤                                     │
```

### 21.3 Execution Cancellation Flow (OCC Conditional Commit)
```text
Client                  FastAPI Router          CancellationUseCase (LLD-06)         PostgreSQL / OCC Conditional Commit
  │                           │                              │                                  │
  │ 1. POST /cancel           │                              │                                  │
  ├──────────────────────────>│                              │                                  │
  │                           │ 2. request_cancel(wf_id)     │                                  │
  │                           ├─────────────────────────────>│                                  │
  │                           │                              │ 3. Read Current State & Rev      │
  │                           │                              ├─────────────────────────────────>│
  │                           │                              │    State == RUNNING, Rev=2       │
  │                           │                              │<─────────────────────────────────┤
  │                           │                              │ 4. OCC Commit: State->CANCELLING │
  │                           │                              │    WHERE id=wf_id AND rev=2      │
  │                           │                              ├─────────────────────────────────>│
  │                           │                              │    COMMITTED (Rev 3)             │
  │                           │                              │<─────────────────────────────────┤
  │                           │ 5. CancellationAccepted      │                                  │
  │                           │<─────────────────────────────┤                                  │
  │ 6. 202 Accepted (DTO)     │                              │                                  │
  │<──────────────────────────┤                              │                                  │
```

---

## 22. Security Threat & Mitigation Matrix

| Threat Description | Attack Boundary | NexusFlow Mitigation Architecture | Realistic Residual Risk | Test Verification |
| :--- | :--- | :--- | :--- | :--- |
| **Credential Timing Attack** | Public & Worker Auth | Precomputed SHA-256 digest compared using `hmac.compare_digest`. | Implementation/runtime side channels outside this comparison path. | `test_auth_timing_constant` |
| **Accidental Token Leakage** | Logging & Metrics | Headers redacted by ASGI middleware; tokens excluded from DTOs and errors. | Log misconfiguration or plaintext interception at reverse proxy layer. | `test_logs_contain_no_secrets` |
| **Public Credential on Worker API** | Worker Endpoint | Authenticator validates credential domain membership; rejects public tokens. | Compromise of configured worker domain credential. | `test_public_token_rejected_on_worker` |
| **Worker Credential on Public API** | Public Endpoint | Public authenticator rejects worker domain tokens with `401 Unauthorized`. | Compromise of configured public API credential. | `test_worker_token_rejected_on_public` |
| **Worker Session ID Spoofing** | Callback Endpoint | Exact durable `(AttemptId, WorkerSessionId)` fencing enforced via DB OCC. | Compromise of trusted worker-domain credential or trusted process. | `test_callback_mismatched_session` |
| **Attempt ID Guessing** | Callback Endpoint | UUIDv4 (122 bits entropy) combined with Bearer auth and session fencing. | Identifier disclosure alone is insufficient, but compromised worker credentials remain sensitive. | `test_callback_unknown_attempt_id` |
| **YAML Bomb / Resource Exhaustion** | Ingestion Adapter | 1 MB body limit, pure-Python parser, max 1000 nodes, 16 depth, no aliases. | CPU consumption within allowed 1 MB bounds on dense text. | `test_yaml_bomb_rejected` |
| **Oversized JSON Payloads** | Ingestion Adapter | 1 MB body guard halts stream; Pydantic limits field sizes. | Bounded ingress bandwidth consumption before 413 disconnect. | `test_oversized_payload_rejected` |
| **Idempotency Token Collision** | Start & Register | Atomic `INSERT ON CONFLICT DO NOTHING` + semantic fingerprint verification. | Client misuse (key reuse across disparate workflows) and application defects. | `test_idempotency_fingerprint_mismatch` |
| **Unauthorized Cancellation** | Public API | Explicit `executions:cancel` permission guard. | Compromise of authorized client credentials. | `test_cancel_permission_enforced` |
| **Plaintext Wire Snooping** | Network | TLS 1.3 mandatory in deployment configuration. | Unencrypted traffic within local container networks if reverse proxy misconfigured. | `test_tls_enforced_in_prod` |

---

## 23. Deterministic Test Strategy

### 23.1 Authentication & Authorization Tests (`tests/integration/api/test_security.py`)
1. **Missing Bearer Header:** Request without `Authorization` header returns `401 Unauthorized` with `WWW-Authenticate` header.
2. **Malformed Bearer Header:** Header `Authorization: Basic xyz` or `Bearer` without token returns `401 Unauthorized`.
3. **Invalid Public Token:** Arbitrary string returns `401 Unauthorized`.
4. **Worker Token on Public Endpoint:** Valid worker token used on `GET /v1/executions` returns `401 Unauthorized`.
5. **Public Token on Worker Endpoint:** Valid public token used on `POST /internal/v1/worker/poll` returns `401 Unauthorized`.
6. **Permission Isolation:**
   - Principal with only `definitions:read` calling `POST /v1/definitions` returns `403 Forbidden`.
   - Principal with `definitions:write` successfully registers definition (`201 Created`).
   - Principal with only `executions:read` calling `POST /v1/executions` returns `403 Forbidden`.
   - Principal with only `executions:read` calling `POST /v1/executions/{execution_id}/cancel` returns `403 Forbidden`.
7. **No Secret Leakage in Error Responses:** Verify response body and headers for 401/403 contain zero substrings of configured tokens.

### 23.2 Definition Registration & Semantic Idempotency Tests (`tests/integration/api/test_definitions.py`)
8. **Valid YAML Registration:** Post valid YAML to `/v1/definitions`; assert `201 Created` and returned `DefinitionResponseDTO` contains valid UUID.
9. **Route Prefix Enforcement:** Calling `POST /api/v1/definitions` returns `404 Not Found`.
10. **Malformed YAML Syntax:** Post invalid YAML syntax; assert `400 Bad Request` with `MALFORMED_YAML`.
11. **Semantic Graph Error:** Post workflow containing cycle or missing dependency; assert `422 Unprocessable Content` with `DEFINITION_VALIDATION_FAILED` and structured diagnostics.
12. **Equivalent YAML Presentation Idempotency:**
    - Submit definition A (standard formatting).
    - Submit definition B (different key order, alternate whitespace, added comments, different quoting) with identical `Idempotency-Key`.
    - **Assert:** Both requests produce identical `RequestFingerprint`, return `201 Created` with the **same** `definition_id`, and exactly 1 row exists in `registered_definitions`.
13. **Conflicting Definition Registration:**
    - Submit definition A with `Idempotency-Key: K1`.
    - Submit definition C (differing in task activity type or dependency) with `Idempotency-Key: K1`.
    - **Assert:** Second request returns `409 Conflict` (`IDEMPOTENCY_CONFLICT`).
14. **Unsupported Content-Type:** Post with `Content-Type: text/plain`; assert `415 Unsupported Media Type`.
15. **Oversized Definition:** Post 1.5 MB YAML; assert `413 Payload Too Large`.

### 23.3 Execution Management Tests (`tests/integration/api/test_executions.py`)
16. **Valid Execution Start:** Post valid `definition_id` and input to `/v1/executions`; assert `201 Created` with state `INITIALIZING` or `RUNNING`.
17. **Start with Non-Existent Definition:** Post unknown `definition_id`; assert `404 Not Found` (`DEFINITION_NOT_FOUND`).
18. **Explicit JSON Null Input:** Post `"workflow_input": null`; assert `201 Created` and execution stores JSON null.
19. **Idempotent Execution Start:** Post identical input with same `Idempotency-Key`; assert second returns identical `workflow_execution_id`.
20. **Conflicting Execution Start:** Post different input with same `Idempotency-Key`; assert `409 Conflict`.
21. **Concurrent Duplicate Starts:** Launch 10 parallel HTTP requests with same `Idempotency-Key`; assert exactly 1 row is created in `workflow_executions`, all 10 receive `201 Created` with identical ID.

### 23.4 Execution Cancellation Tests (`tests/integration/api/test_cancellation.py`)
22. **Cancel Running Execution:** Post cancel to `RUNNING` workflow; assert `202 Accepted` and state `CANCELLING`.
23. **Cancellation Uses OCC Arbitration:** Verify cancellation transaction commits conditionally against `expected_revision` without holding pessimistic `FOR UPDATE` lock.
24. **Duplicate Cancel:** Post cancel to already `CANCELLING` workflow; assert `200 OK` and state `CANCELLING`.
25. **Cancel Terminal Cancelled:** Post cancel to `CANCELLED` workflow; assert `200 OK` and state `CANCELLED`.
26. **Cancel Succeeded Execution:** Post cancel to `SUCCEEDED` workflow; assert `409 Conflict` (`EXECUTION_NOT_CANCELLABLE`).
27. **Cancel Failed Execution:** Post cancel to `FAILED` workflow; assert `409 Conflict` (`EXECUTION_NOT_CANCELLABLE`).

### 23.5 Recovery Admission Tests (`tests/integration/api/test_recovery_gate.py`)
28. **Mutations Blocked When Gate Closed:** Set `RecoveryGate.allows_new_work = False`; assert `POST /v1/definitions`, `POST /v1/executions`, and `POST /v1/executions/{id}/cancel` return `503 Service Unavailable` (`NOT_READY`).
29. **Reads Allowed When Gate Closed:** Set `RecoveryGate.allows_new_work = False`; assert `GET /v1/executions` and `GET /v1/definitions` return `200 OK`.
30. **Readiness Probe Under Recovery:** Assert `GET /readyz` returns `503` while gate is closed, and flips to `200 OK` when opened.
31. **Late Worker Callback Admitted While Gate Closed:** Verify `POST /internal/v1/worker/callback` is admitted to race OCC during recovery per LLD-07 exception.

### 23.6 Output Semantics & Pagination Tests (`tests/integration/api/test_views.py`)
32. **Output Absence vs. JSON Null:**
    - Query `RUNNING` task; assert `"has_output": false` and `"output"` is omitted or null.
    - Query `SUCCEEDED` task with explicit null; assert `"has_output": true` and `"output": null`.
    - Query `SUCCEEDED` task with payload; assert `"has_output": true` and `"output": {"data": 123}`.
33. **Keyset Cursor Traversal:** Seed 250 executions; fetch pages with `limit=50`; traverse using `next_cursor`; assert all 250 unique executions visited without gaps or duplicates.
34. **Malformed Cursor:** Pass invalid Base64 string as `cursor`; assert `400 Bad Request` (`INVALID_CURSOR`).
35. **Stable History Pagination on Timestamp & ID:** Seed 100 history entries with identical `occurred_at_utc` timestamps; paginate across pages; assert all 100 entries visited without gaps, duplicates, or order drift using `(occurred_at_utc, history_id)` keyset.
36. **No Sequence-Number in History Response:** Assert serialized `HistoryEntryResponseDTO` payload has no `sequence_number` key.
37. **History Non-Authoritative Verification:** Verify history endpoints remain strictly read-only audit representations and do not trigger scheduler wakeups, attempt state mutations, or recovery repair.

---

## 24. Explicit Non-Goals

LLD-08 strictly excludes:
1. **OAuth2 / OIDC Providers:** No authorization servers, client credential grants, or token exchange endpoints.
2. **User Management / Sign-Up:** No user registration, password hashing, or database user tables.
3. **JWT Complexity:** No asymmetric key rotation, claims evaluation, or token refresh flows.
4. **Browser Sessions / Cookies:** No cookie-based auth, CSRF tokens, or browser-specific state.
5. **Per-Tenant Resource Isolation:** Multi-tenancy is deferred; single-tenant administrative domain.
6. **Rate Limiting / Quotas:** No Redis-backed token buckets or throttling middleware in V1.
7. **Workflow Versioning:** Immutability requires new definition IDs; no in-place version migration.
8. **WebSockets / SSE / Webhooks:** Communication is strictly pull-based HTTP request/response.
9. **GraphQL / Custom Query Engines:** Querying is strictly RESTful with bounded keyset pagination.
10. **Generic Lifecycle Mutation (`PATCH`):** Clients cannot mutate workflow or task states directly.

---

## 25. Implementation Checklist

- [x] **FastAPI Application Factory:** Configure CORS, route mounting beneath `/v1`, and OpenAPI tags.
- [x] **Route Prefix Verification:** Ensure `/v1` is sole public prefix; `/api/v1` returns 404.
- [x] **Request Size Middleware:** ASGI guard enforcing 1 MB limit returning `413`.
- [x] **Request Correlation Middleware:** UUIDv4 generation, logging injection, and `X-Request-ID` header.
- [x] **Public Bearer Authenticator:** SHA-256 precomputation and constant-time `compare_digest`.
- [x] **Permission Guards:** `require_permission` dependency checking `SecurityContext`.
- [x] **Recovery Gate Guards:** `require_new_work_admitted` dependency enforcing LLD-07 admission.
- [x] **DTO Implementation:** Strict Pydantic v2 models with `extra="forbid"` and frozen configs.
- [x] **History DTO Alignment:** `HistoryEntryResponseDTO` reflects frozen `history_entries` schema without `sequence_number`.
- [x] **Definition Registration Route:** Integration with LLD-03 `DefinitionRegistrationUseCase`.
- [x] **Semantic Idempotency Fingerprint:** Fingerprinting canonical `ValidatedWorkflowSpec` serialization, invariant to YAML presentation.
- [x] **Execution Start Route:** Integration with LLD-01 `StartExecutionUseCase` and LLD-02 persistence.
- [x] **Execution Cancellation Route:** Integration with LLD-06 cancellation use cases using OCC conditional commit.
- [x] **Resource Listing Routes:** Read-only inspection for Definitions, Executions, Tasks, Attempts, History.
- [x] **Opaque Keyset Pagination:** `CursorCodec` implementation with limit caps; history pagination keyed on `(occurred_at_utc, history_id)`.
- [x] **Output Serialization:** Distinction between `has_output: false` and `has_output: true, output: null`.
- [x] **Centralized Exception Handler:** Mapping domain/application exceptions to standard error envelopes.
- [x] **Health Probes:** Minimalistic `/healthz` and `/readyz` endpoints.
- [x] **Automated Integration Tests:** Comprehensive suite covering security, idempotency, history pagination, and lifecycle routes.

---

## 26. Cross-LLD Traceability Matrix

| LLD-08 Concern | Authoritative ADR | Upstream LLD Dependency | Implementation Module | Test Suite Verification |
| :--- | :--- | :--- | :--- | :--- |
| Public API Endpoints (`/v1`) | ADR-015 | LLD-01 (Commands & Results) | `interfaces/http/public/*.py` | `test_definitions.py`, `test_executions.py` |
| Security & Credentials | ADR-022 | LLD-01 (`SecurityContext`) | `interfaces/http/security.py` | `test_security.py` |
| Definition Ingestion & Semantic Fingerprint | ADR-002, 004 | LLD-03 (`DefinitionRegistrationUseCase`) | `interfaces/http/public/definitions.py` | `test_definitions.py` |
| Idempotency Records | ADR-015, 020 | LLD-02 (`idempotency_records`)| `interfaces/http/public/executions.py` | `test_executions.py` |
| Cancellation Semantics (OCC) | ADR-013, 015 | LLD-06 (`CancellationUseCase`)| `interfaces/http/public/executions.py` | `test_cancellation.py` |
| Recovery Gate Gating | ADR-012 | LLD-07 (`RecoveryGate`) | `interfaces/http/dependencies.py` | `test_recovery_gate.py` |
| Error Envelope & Codes | ADR-018 | LLD-01 (`FailureCategory`) | `interfaces/http/errors.py` | `test_errors.py` |
| Output Null Semantics | ADR-010 | LLD-02 (`has_output` column) | `interfaces/http/dto.py` | `test_views.py` |
| Keyset Pagination & History Ordering | ADR-014, 015 | LLD-02 (`history_entries` keyset) | `interfaces/http/cursor.py` | `test_views.py` |
| Worker Separation | ADR-022 | LLD-05 (`WorkerRegistryPort`) | `interfaces/http/security.py` | `test_security.py` |

---

### Classification

**LLD-08 — Architecture-Ready / Approved as LLD-09 Input**
