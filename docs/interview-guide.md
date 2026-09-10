# NexusFlow V1 — Systems & Architecture Interview Guide

This guide prepares engineers to discuss and defend the architectural, persistence, and concurrency decisions in **NexusFlow V1**. NexusFlow is a correctness-focused, portfolio-grade distributed workflow orchestration engine with a single control plane and distributed external workers, backed by PostgreSQL 16 as the sole durable authority.

---

## 1. Core Architectural Concepts

### Q1: Why is PostgreSQL the sole durable authority? Why not Redis, Kafka, or an event-sourcing log?
**Answer:**
1. **Zero Split-Brain Ambiguity:** Relying on an external message broker (Kafka/RabbitMQ) and a database introduces dual-write consistency hazards. In partial failure scenarios, reconciling out-of-sync message offsets with database rows requires complex two-phase commits.
2. **ACID Transactions as the Coordinator:** By modeling task offers, claims, attempts, and heartbeats directly in PostgreSQL, state transitions and invariant checks occur atomically in a single READ COMMITTED transaction.
3. **Auditability Without Eventual Consistency:** History is stored as an immutable audit trail written within the same transaction that commits a state change. The active state is stored as concrete, strongly-typed rows, eliminating the need to replay thousands of events on startup to reconstruct state.

---

### Q2: How does NexusFlow prevent race conditions when multiple workers attempt to claim the same task?
**Answer:**
NexusFlow enforces a strict two-phase ownership model: **Candidate/Offer is NOT ownership; Attempt creation is the ownership commit.**
1. During scheduler polling, multiple active workers may receive an offer for a RUNNABLE task.
2. The first worker to call POST /v1/worker/tasks/{id}/claim initiates commit_task_claim.
3. Inside PostgreSQL, this executes an UPDATE task_executions SET state = 'CLAIMED', revision = revision + 1 WHERE task_execution_id = :id AND state = 'RUNNABLE' AND revision = :expected_revision.
4. If two workers race, exactly one worker’s update will affect 1 row. The losing worker receives 0 affected rows, triggering an OCC_CONFLICT outcome.
5. The winning transaction atomically inserts an execution_attempts record with ttempt_number = 1, durable worker session fencing, and a strict start_deadline_utc. The losing worker safely backs off and polls for other tasks.

---

### Q3: When do you use Optimistic Concurrency Control (OCC) vs. Pessimistic Row Locking (SELECT ... FOR UPDATE)?
**Answer:**
- **OCC (evision = :expected_revision):** Used for normal execution lifecycle operations—task polling, claim commits, start commits, heartbeat renewals, and successful completions. Because task instances are owned by a single worker session, write contention during normal execution is minimal.
- **Narrow Row Locks (SELECT ... FOR UPDATE):** Strictly limited to **workflow direction boundaries**:
  - RUNNING -> FAILING (when an attempt failure causes retry budget exhaustion).
  - INITIALIZING/RUNNING -> CANCELLING (when a user or system issues a cancel command).
  **Why?** To prevent a race between a failing workflow draining sibling tasks and an in-flight worker attempting to start or complete another task in the same workflow. Lock ordering always proceeds in canonical order: owning workflow_executions row locked first, then child 	ask_executions and execution_attempts.

---

### Q4: How does Crash Recovery work if the Control Plane crashes mid-execution?
**Answer:**
NexusFlow implements a stateless StartupRecoveryEngine that scans PostgreSQL upon control plane boot (or periodically):
1. **INITIALIZING Workflows:** Repaired by inspecting task population completeness and transitioning to RUNNING.
2. **Orphaned CLAIMED Tasks:** If a worker claimed a task but died before calling start, its start_deadline_utc expires. The recovery engine fails the attempt with START_TIMEOUT and resets the task to RUNNABLE or RETRY_WAIT.
3. **Dead Worker Sessions:** If heartbeats cease, the worker's session is marked dead. The recovery engine detects running attempts tied to dead sessions, marks them FAILED (WORKER_LOSS), and schedules retries.
4. **Draining Workflows:** Workflows stuck in FAILING or CANCELLING are re-drained by cancelling all unstarted tasks and transitioning the workflow to terminal FAILED or CANCELLED.
Durability across real restarts is empirically validated via automated container restart integration tests (	est_postgres_container_restart.py).

---

### Q5: Why is History an audit trail rather than an event source?
**Answer:**
In pure Event Sourcing, the state is derived by replaying past events. In workflow engines, replaying historical events on reboot introduces startup latency and non-deterministic state recovery if event schemas evolve.
NexusFlow stores state authoritatively in normal relational tables (workflow_executions, 	ask_executions, execution_attempts). The history_entries table is written append-only in the same database transaction solely for auditing, tracing, and user visibility.

---

### Q6: How does NexusFlow guarantee idempotency in worker callbacks?
**Answer:**
Every worker callback (claim, start, heartbeat, succeed, ail, cancel_ack) includes:
1. 	ask_execution_id
2. ttempt_id
3. worker_session_id
4. Expected evision

If a network timeout causes a worker to retry an HTTP callback that already succeeded:
- The transaction checks if the attempt is already in the target state with identical output payload. If so, it returns 200 OK (idempotent duplicate acceptance) without modifying state or incrementing revisions.
- If the session token or attempt ID does not match, the request is rejected with 409 Conflict or 403 Forbidden.
