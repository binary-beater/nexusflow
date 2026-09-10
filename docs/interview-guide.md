# NexusFlow V1 — Systems & Architecture Interview Guide

This guide prepares engineers to discuss and defend the architectural, persistence, and concurrency decisions in **NexusFlow V1**. NexusFlow is a correctness-focused distributed workflow orchestration engine with a single control plane and distributed external workers, backed by PostgreSQL 16 as the sole durable authority.

---

## 1. Core Architectural Concepts

### Q1: Why is PostgreSQL the sole durable authority? Why not Redis, Kafka, or an event-sourcing log?
**Answer:**
1. **Zero Split-Brain Ambiguity:** Relying on an external message broker (Kafka/RabbitMQ) and a database introduces dual-write consistency hazards. In partial failure scenarios, reconciling out-of-sync message offsets with database rows requires complex two-phase commits.
2. **ACID Transactions as the Coordinator:** By modeling task dispatch, claims, attempts, and heartbeats directly in PostgreSQL, state transitions and invariant checks occur atomically in a single READ COMMITTED transaction.
3. **Auditability Without Eventual Consistency:** History is stored as an immutable audit trail written within the same transaction that commits a state change. The active state is stored as concrete, strongly-typed rows, eliminating the need to replay thousands of events on startup to reconstruct state.

---

### Q2: How does NexusFlow prevent race conditions when multiple workers attempt to claim the same task?
**Answer:**
NexusFlow enforces a strict two-phase ownership model: **Candidate/Offer is NOT ownership; Attempt creation is the ownership commit.**
1. During scheduler dispatch, live workers registered in the in-memory WorkerRegistry are matched against RUNNABLE tasks.
2. The scheduler executes the atomic ownership commit `commit_attempt_ownership` in PostgreSQL:
   ```sql
   UPDATE task_executions
   SET state = 'RUNNING',
       revision = revision + 1,
       next_attempt_ordinal = next_attempt_ordinal + 1,
       updated_at_utc = :now_utc
   WHERE task_execution_id = :task_id
     AND workflow_execution_id = :workflow_id
     AND state = 'RUNNABLE'
     AND revision = :expected_task_revision
   RETURNING next_attempt_ordinal - 1;
   ```
3. In the exact same database transaction, a new `execution_attempts` record is inserted in `CLAIMED` state with the allocated ordinal, worker session ID, and `start_deadline_utc`.
4. If multiple dispatchers or concurrent reconciliation cycles race for the same task, strict OCC on `task_executions.revision` ensures exactly one commit succeeds; the losing transaction receives 0 affected rows (`OCC_CONFLICT`) and safely backs off.
5. The worker receives the assignment via long-poll (`POST /internal/v1/worker/poll`) and acknowledges start via `POST /internal/v1/worker/start`, which conditionally transitions the attempt from `CLAIMED` to `RUNNING` before executing activity code.

---

### Q3: When do you use Optimistic Concurrency Control (OCC) vs. Pessimistic Row Locking (SELECT ... FOR UPDATE)?
**Answer:**
- **OCC (`revision = :expected_revision`):** Used for normal execution lifecycle operations—task polling, claim commits, start commits, heartbeat renewals, and successful completions. Because task instances are owned by a single worker session, write contention during normal execution is minimal.
- **Narrow Row Locks (`SELECT ... FOR UPDATE`):** Strictly limited to **workflow direction boundaries**:
  - RUNNING -> FAILING (when an attempt failure causes retry budget exhaustion).
  - INITIALIZING/RUNNING -> CANCELLING (when a user or system issues a cancel command).
  **Why?** To prevent a race between a failing workflow draining sibling tasks and an in-flight worker attempting to start or complete another task in the same workflow. Lock ordering always proceeds in canonical order: owning `workflow_executions` row locked first, then child `task_executions` and `execution_attempts`.

---

### Q4: How does Crash Recovery work if the Control Plane crashes mid-execution?
**Answer:**
NexusFlow implements a stateless StartupRecoveryEngine that scans PostgreSQL upon control plane boot (or periodically):
1. **INITIALIZING Workflows:** Repaired by inspecting task population completeness and transitioning to RUNNING.
2. **Orphaned CLAIMED Tasks:** If a worker claimed a task but died before calling start, its `start_deadline_utc` expires. The recovery engine fails the attempt with START_TIMEOUT and resets the task to RUNNABLE or RETRY_WAIT.
3. **Dead Worker Sessions:** If heartbeats cease, the worker's session is marked dead. The recovery engine detects running attempts tied to dead sessions, marks them FAILED (WORKER_LOSS), and schedules retries.
4. **Draining Workflows:** Workflows stuck in FAILING or CANCELLING are re-drained by cancelling all unstarted tasks and transitioning the workflow to terminal FAILED or CANCELLED.
Durability across real restarts is empirically validated via automated container restart integration tests (`test_postgres_container_restart.py`).

---

### Q5: Why is History an audit trail rather than an event source?
**Answer:**
In pure Event Sourcing, the state is derived by replaying past events. In workflow engines, replaying historical events on reboot introduces startup latency and non-deterministic state recovery if event schemas evolve.
NexusFlow stores state authoritatively in normal relational tables (`workflow_executions`, `task_executions`, `execution_attempts`). The `history_entries` table is written append-only in the same database transaction solely for auditing, tracing, and user visibility.

---

### Q6: How does NexusFlow guarantee idempotency in worker callbacks?
**Answer:**
Every worker callback (`/start`, `/callback`) includes:
1. `task_execution_id`
2. `attempt_id`
3. `worker_session_id`
4. Expected `revision`

If a network timeout causes a worker to retry an HTTP callback that already succeeded:
- The transaction checks if the attempt is already in the target state with identical output payload. If so, it returns 200 OK (idempotent duplicate acceptance) without modifying state or incrementing revisions.
- If the session token or attempt ID does not match, the request is rejected with 409 Conflict or 403 Forbidden.

---

### Q7: What are the measured performance characteristics and true ownership latency?
**Answer:**
In local benchmarks against real PostgreSQL 16 (Intel Core i7-1165G7 @ 2.80GHz, 15.56 GB RAM):
1. **Continuous Workload Scale:** Executed **350 continuous workflows** comprising **1,050 durable task executions** per run across 3 consecutive runs (3,150 total durable tasks) with 100% completion (350/350 workflows succeeded).
2. **Throughput:** Achieved **~13.12 tasks/sec** (~4.37 workflows/sec) for a durable 3-stage linear pipeline.
3. **True Ownership Latency:** Measured exact delta between `TaskExecution` becoming durably `RUNNABLE` and the `commit_attempt_ownership` transaction committing `Attempt` as `CLAIMED`:
   - **Median (p50):** 11.69 ms
   - **95th percentile (p95):** 17.86 ms
   - **99th percentile (p99):** 22.39 ms
4. **Client-to-Claim vs. True Ownership:** Differentiated round-trip client submission-to-claim latency (~116.76 ms p50) from true internal ownership latency (11.69 ms p50), reflecting exact PostgreSQL commit boundaries.
