from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from nexusflow.domain.identifiers import ActivityType, TaskDefinitionId
from nexusflow.domain.json_compat import JsonValue

# --- Input Bindings (ADR-010 / LLD-01 / LLD-03) ---


@dataclass(frozen=True, slots=True)
class LiteralBinding:
    """Injects a static, immutable JSON-compatible literal."""

    value: JsonValue


@dataclass(frozen=True, slots=True)
class WorkflowInputBinding:
    """Binds the entire immutable workflow execution input value."""

    pass


@dataclass(frozen=True, slots=True)
class TaskOutputBinding:
    """Binds the entire committed output of a direct upstream dependency task."""

    upstream_task_id: TaskDefinitionId


type InputBinding = LiteralBinding | WorkflowInputBinding | TaskOutputBinding

# --- Workflow Output Bindings (ADR-010 / LLD-01 / LLD-03) ---


@dataclass(frozen=True, slots=True)
class WorkflowTaskOutputBinding:
    """References the authoritative whole-value output of any task defined in the workflow."""

    source_task_id: TaskDefinitionId


@dataclass(frozen=True, slots=True)
class WorkflowInputPassthroughBinding:
    """Passes through the entire immutable workflow execution input value."""

    pass


@dataclass(frozen=True, slots=True)
class WorkflowLiteralOutputBinding:
    """Binds a static literal value to a named workflow output."""

    value: JsonValue


type WorkflowOutputBinding = (
    WorkflowTaskOutputBinding | WorkflowInputPassthroughBinding | WorkflowLiteralOutputBinding
)

# --- Task & Specification Structures ---


@dataclass(frozen=True, slots=True)
class TaskDefinition:
    id: TaskDefinitionId
    activity_type: ActivityType
    dependencies: frozenset[TaskDefinitionId]
    input_bindings: Mapping[str, InputBinding]
    max_attempts: int

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")


@dataclass(frozen=True, slots=True)
class CandidateWorkflowSpec:
    """Unvalidated candidate specification resulting from structural normalization.

    Preserves raw user dependency declarations so the semantic validator can produce accurate diagnostics.
    """

    workflow_name: str
    tasks: Sequence[TaskDefinition]
    raw_dependencies: Mapping[TaskDefinitionId, tuple[str, ...]]
    output_bindings: Mapping[str, WorkflowTaskOutputBinding]


@dataclass(frozen=True, slots=True)
class ValidatedWorkflowSpec:
    """Immutable, semantically validated specification.

    Guaranteed by ADR-004 to represent an acyclic DAG with all dependencies,
    activity types, and whole-value bindings verified.
    """

    workflow_name: str
    tasks: Mapping[TaskDefinitionId, TaskDefinition]
    output_bindings: Mapping[str, WorkflowTaskOutputBinding]
