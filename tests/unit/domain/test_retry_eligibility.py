"""Unit tests for Phase 3 Failure Classification and Retry Eligibility (LLD-06)."""

from nexusflow.domain.enums import FailureCategory, WorkflowState
from nexusflow.domain.failure import (
    WorkerFailureReport,
    classify_worker_failure,
    evaluate_retry_eligibility,
)


def test_evaluate_retry_eligibility_when_workflow_running_and_attempts_remain():
    # Attempt 1 failed, next is 2, max 3 -> eligible
    eligible = evaluate_retry_eligibility(
        workflow_state=WorkflowState.RUNNING,
        is_classified_retryable=True,
        next_attempt_ordinal=2,
        max_attempts=3,
    )
    assert eligible is True


def test_evaluate_retry_eligibility_when_max_attempts_reached():
    # Attempt 3 failed, next is 4, max 3 -> NOT eligible
    eligible = evaluate_retry_eligibility(
        workflow_state=WorkflowState.RUNNING,
        is_classified_retryable=True,
        next_attempt_ordinal=4,
        max_attempts=3,
    )
    assert eligible is False


def test_evaluate_retry_eligibility_when_workflow_not_running():
    # Workflow in CANCELLING -> NOT eligible
    eligible = evaluate_retry_eligibility(
        workflow_state=WorkflowState.CANCELLING,
        is_classified_retryable=True,
        next_attempt_ordinal=2,
        max_attempts=3,
    )
    assert eligible is False

    # Workflow in FAILING -> NOT eligible
    eligible_failing = evaluate_retry_eligibility(
        workflow_state=WorkflowState.FAILING,
        is_classified_retryable=True,
        next_attempt_ordinal=2,
        max_attempts=3,
    )
    assert eligible_failing is False


def test_classify_worker_failure_transient_vs_fatal():
    # Transient error
    rep_transient = WorkerFailureReport(
        error_code="NETWORK_DISCONNECT",
        error_message="Network dropped",
    )
    cause_transient, retryable_transient = classify_worker_failure(rep_transient)
    assert retryable_transient is True
    assert cause_transient.category == FailureCategory.SYSTEM_TRANSIENT

    # Fatal error (validation)
    rep_validation = WorkerFailureReport(
        error_code="BAD_INPUT_SCHEMA",
        error_message="Bad data schema",
    )
    cause_val, retryable_val = classify_worker_failure(rep_validation)
    assert retryable_val is False
    assert cause_val.category == FailureCategory.VALIDATION
