"""Prometheus metrics catalog with low-cardinality label guards (LLD-09 Section 10.2)."""

from prometheus_client import Counter, Gauge, Histogram

# HTTP metrics
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "nexusflow_http_request_duration_seconds",
    "HTTP request latency by route template and status code",
    ["method", "route", "status_code"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# Workflow executions
WORKFLOW_TRANSITION_TOTAL = Counter(
    "nexusflow_workflow_transition_total",
    "Workflow transitions by target state",
    ["to_state"],
)

WORKFLOW_EXECUTION_DURATION_SECONDS = Histogram(
    "nexusflow_workflow_execution_duration_seconds",
    "Workflow end-to-end duration from creation to terminal state",
    ["terminal_state"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

# Task executions
TASK_TRANSITION_TOTAL = Counter(
    "nexusflow_task_transition_total",
    "Task transitions by target state",
    ["to_state"],
)

TASK_EXECUTION_DURATION_SECONDS = Histogram(
    "nexusflow_task_execution_duration_seconds",
    "Task duration from RUNNABLE to terminal state",
    ["terminal_state"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# Attempts & ownership
ATTEMPT_SETTLEMENT_TOTAL = Counter(
    "nexusflow_attempt_settlement_total",
    "Attempt settlement counts by trigger and result",
    ["trigger", "settlement_result"],
)

SCHEDULING_LATENCY_SECONDS = Histogram(
    "nexusflow_scheduling_latency_seconds",
    "Latency between task becoming RUNNABLE and successful ownership commit (CLAIMED)",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0),
)

# Workers & sessions
ACTIVE_WORKER_SESSIONS = Gauge(
    "nexusflow_active_worker_sessions",
    "Current live worker sessions registered in ephemeral memory",
)

WORKER_SESSIONS_TOTAL = Counter(
    "nexusflow_worker_sessions_total",
    "Worker sessions registered by outcome",
    ["event"],  # 'registered', 'lost', 'unregistered'
)

# Concurrency & Database
DATABASE_OPERATION_DURATION_SECONDS = Histogram(
    "nexusflow_db_operation_duration_seconds",
    "PostgreSQL transaction latency by operation name",
    ["operation", "status"],
    buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

OCC_CONFLICT_TOTAL = Counter(
    "nexusflow_occ_conflict_total",
    "Optimistic concurrency control conflict retries by entity",
    ["entity_type"],
)

# Recovery
RECOVERY_RUN_DURATION_SECONDS = Histogram(
    "nexusflow_recovery_run_duration_seconds",
    "Duration of startup recovery sweeps by phase",
    ["phase"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

RECOVERY_MUTATIONS_TOTAL = Counter(
    "nexusflow_recovery_mutations_total",
    "Total entities repaired during startup recovery sweeps",
    ["phase"],
)
