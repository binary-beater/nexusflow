from dataclasses import dataclass

from nexusflow.domain.enums import FailureCategory, WorkflowState
from nexusflow.domain.json_compat import JsonObject


@dataclass(frozen=True, slots=True)
class WorkerFailureReport:
    """Untrusted raw observation submitted by a worker. Worker never decides retryability."""

    error_code: str
    error_message: str
    details: JsonObject | None = None


@dataclass(frozen=True, slots=True)
class FailureCause:
    """Durable failure cause metadata according to ADR-018."""

    category: FailureCategory
    code: str
    message: str
    details: JsonObject | None = None


def evaluate_retry_eligibility(
    *,
    next_attempt_ordinal: int,
    max_attempts: int,
    is_classified_retryable: bool,
    workflow_state: WorkflowState | str,
) -> bool:
    """Evaluates whether a task failure is eligible for retry under engine policy.

    Invariants:
    - WorkflowExecution must be RUNNING.
    - Failure must be classified retryable by engine policy.
    - Attempt budget must remain: next_attempt_ordinal <= max_attempts.
    """
    wf_state = workflow_state.value if isinstance(workflow_state, WorkflowState) else workflow_state
    if wf_state != "RUNNING":
        return False
    if not is_classified_retryable:
        return False
    return next_attempt_ordinal <= max_attempts


def classify_worker_failure(report: WorkerFailureReport) -> tuple[FailureCause, bool]:
    """Pure engine classification of worker failure reports into FailureCause and retryability."""
    code = report.error_code.strip() if report.error_code else "UNKNOWN_ERROR"
    msg = report.error_message.strip() if report.error_message else "Activity failure reported by worker"

    # Engine classification rules (LLD-06 Section 3.3)
    if any(k in code.upper() for k in ("TRANSIENT", "IO_ERROR", "NETWORK", "CONNECTION", "SOCKET", "OSERROR", "TIMEOUT")):
        category = FailureCategory.SYSTEM_TRANSIENT
        retryable = True
    elif "UNAVAILABLE" in code.upper() or "CRASH" in code.upper():
        category = FailureCategory.WORKER_AVAILABILITY
        retryable = True
    elif "TIMEOUT" in code.upper() or "DEADLINE" in code.upper():
        category = FailureCategory.TIME_BASED
        retryable = True
    elif "VALIDATION" in code.upper() or "BAD_INPUT" in code.upper():
        category = FailureCategory.VALIDATION
        retryable = False
    elif "SECURITY" in code.upper() or "UNAUTHORIZED" in code.upper():
        category = FailureCategory.SECURITY
        retryable = False
    elif "INTEGRITY" in code.upper():
        category = FailureCategory.INTEGRITY
        retryable = False
    else:
        category = FailureCategory.DOMAIN_EXECUTION
        # Domain execution failures retryable if code contains 'RETRY' or 'TRANSIENT'
        retryable = "RETRY" in code.upper()

    cause = FailureCause(
        category=category,
        code=code,
        message=msg[:4096],
        details=report.details,
    )
    return cause, retryable
