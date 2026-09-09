"""Public API and Worker HTTP boundary DTOs (LLD-05, LLD-08)."""

from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# =====================================================================
# Public Definition & Execution DTOs (LLD-08)
# =====================================================================


class RegisterDefinitionJsonDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    yaml_content: str = Field(..., min_length=1, max_length=1_000_000)


class CreateExecutionRequestDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    definition_id: UUID
    workflow_input: Any = Field(default=None)


class DefinitionResponseDTO(BaseModel):
    model_config = ConfigDict(frozen=True)
    definition_id: UUID
    workflow_name: str
    spec_version: str = "v1"
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
    """Public representation of an immutable audit trail entry.

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


# =====================================================================
# Internal Worker Protocol DTOs (LLD-05 Section 9.2)
# =====================================================================


class StrictWorkerDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# 1. Registration
class WorkerRegistrationRequestDTO(StrictWorkerDTO):
    worker_id: str = Field(min_length=1, max_length=256)
    capabilities: list[str] = Field(min_length=1, max_length=1000)
    client_version: str = Field(default="1.0.0", max_length=64)


class WorkerRegistrationResponseDTO(StrictWorkerDTO):
    worker_session_id: UUID
    status: Literal["REGISTERED"] = "REGISTERED"
    heartbeat_interval_seconds: float = 5.0
    worker_liveness_timeout_seconds: float = 15.0


# 2. Heartbeat
class WorkerHeartbeatRequestDTO(StrictWorkerDTO):
    worker_session_id: UUID
    accepting_new_work: bool = True


class WorkerHeartbeatResponseDTO(StrictWorkerDTO):
    status: Literal["ACCEPTED"] = "ACCEPTED"
    control_plane_draining: bool = False


# 3. Polling
class WorkerPollRequestDTO(StrictWorkerDTO):
    worker_session_id: UUID
    accepting_new_work: bool = True
    max_items: int = Field(default=1, ge=1, le=10)
    timeout_seconds: float = Field(default=20.0, ge=0.1, le=60.0)


class TaskAssignmentPayloadDTO(StrictWorkerDTO):
    attempt_id: UUID
    attempt_ordinal: int
    task_execution_id: UUID
    workflow_execution_id: UUID
    worker_session_id: UUID
    activity_type: str
    stable_input: dict[str, Any]
    start_deadline_utc: str


class TaskCancellationPayloadDTO(StrictWorkerDTO):
    attempt_id: UUID
    worker_session_id: UUID
    task_execution_id: UUID
    workflow_execution_id: UUID
    reason: str


class WorkerPollResponseDTO(StrictWorkerDTO):
    status: Literal["ASSIGNMENT", "CANCEL_COMMAND", "NO_WORK"]
    assignment: TaskAssignmentPayloadDTO | None = None
    cancellation: TaskCancellationPayloadDTO | None = None


# 4. Start Acknowledgement
class WorkerStartAckRequestDTO(StrictWorkerDTO):
    attempt_id: UUID
    worker_session_id: UUID


class WorkerStartAckResponseDTO(StrictWorkerDTO):
    status: Literal["ACCEPTED", "IDEMPOTENT_ALREADY_RUNNING"]
    attempt_id: UUID
    attempt_state: Literal["RUNNING"] = "RUNNING"


# 5. Result Callbacks
class ActivitySuccessPayloadDTO(StrictWorkerDTO):
    outcome_type: Literal["SUCCESS"] = "SUCCESS"
    output: Any  # Must be strictly JSON-compatible


class ActivityFailureDetailsDTO(StrictWorkerDTO):
    error_type: str = Field(max_length=256)
    message: str = Field(max_length=4096)
    details: dict[str, Any] | None = None


class ActivityFailurePayloadDTO(StrictWorkerDTO):
    outcome_type: Literal["FAILURE"] = "FAILURE"
    error: ActivityFailureDetailsDTO


class ActivityCancelAckPayloadDTO(StrictWorkerDTO):
    outcome_type: Literal["CANCEL_ACK"] = "CANCEL_ACK"
    observed_state: Literal["COOPERATIVELY_STOPPED", "ALREADY_COMPLETED", "NOT_FOUND"]


WorkerCallbackOutcomeDTO = Annotated[
    ActivitySuccessPayloadDTO | ActivityFailurePayloadDTO | ActivityCancelAckPayloadDTO,
    Field(discriminator="outcome_type"),
]


class WorkerCallbackRequestDTO(StrictWorkerDTO):
    attempt_id: UUID
    worker_session_id: UUID
    payload: WorkerCallbackOutcomeDTO


class WorkerCallbackResponseDTO(StrictWorkerDTO):
    status: Literal["ACCEPTED", "IDEMPOTENT_DUPLICATE"]
    attempt_id: UUID
