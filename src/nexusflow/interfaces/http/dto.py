"""Public API and Worker HTTP boundary DTOs (LLD-08 Section 14)."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


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
