"""Pure readiness evaluator and input/output binding resolution (ADR-005, ADR-010, LLD-04)."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

from nexusflow.domain.identifiers import TaskDefinitionId, TaskExecutionId, WorkflowExecutionId
from nexusflow.domain.json_compat import JsonObject, JsonValue
from nexusflow.domain.spec import (
    LiteralBinding,
    TaskDefinition,
    TaskOutputBinding,
    WorkflowInputBinding,
    WorkflowTaskOutputBinding,
)


class ReadinessReason(StrEnum):
    READY = "READY"
    WORKFLOW_NOT_RUNNING = "WORKFLOW_NOT_RUNNING"
    TASK_NOT_PENDING = "TASK_NOT_PENDING"
    DEPENDENCIES_INCOMPLETE = "DEPENDENCIES_INCOMPLETE"
    DEPENDENCY_NOT_SUCCEEDED = "DEPENDENCY_NOT_SUCCEEDED"
    OUTPUT_NOT_AVAILABLE = "OUTPUT_NOT_AVAILABLE"
    INPUT_RESOLUTION_CORRUPT = "INPUT_RESOLUTION_CORRUPT"


@dataclass(frozen=True, slots=True)
class DependencyStateSnapshot:
    task_definition_id: TaskDefinitionId
    state: str  # "PENDING", "RUNNABLE", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"
    has_output: bool
    output_value: JsonValue | None


@dataclass(frozen=True, slots=True)
class TaskReadinessSnapshot:
    workflow_id: WorkflowExecutionId
    workflow_state: str
    workflow_input: JsonValue
    task_id: TaskExecutionId
    task_definition_id: TaskDefinitionId
    task_state: str
    task_revision: int
    task_definition: TaskDefinition
    upstream_dependencies: Mapping[TaskDefinitionId, DependencyStateSnapshot]


@dataclass(frozen=True, slots=True)
class ReadinessDecision:
    ready: bool
    resolved_input: JsonObject | None
    reason: ReadinessReason
    diagnostic_message: str = ""


def evaluate_task_readiness(snapshot: TaskReadinessSnapshot) -> ReadinessDecision:
    """Pure, deterministic evaluation of task eligibility and stable input resolution.

    Does NOT perform I/O, lock acquisitions, or state mutations.
    """
    if snapshot.workflow_state != "RUNNING":
        return ReadinessDecision(
            ready=False,
            resolved_input=None,
            reason=ReadinessReason.WORKFLOW_NOT_RUNNING,
            diagnostic_message=f"Workflow is in state '{snapshot.workflow_state}', not 'RUNNING'.",
        )

    if snapshot.task_state != "PENDING":
        return ReadinessDecision(
            ready=False,
            resolved_input=None,
            reason=ReadinessReason.TASK_NOT_PENDING,
            diagnostic_message=f"Task is in state '{snapshot.task_state}', not 'PENDING'.",
        )

    # Verify all declared direct dependencies are SUCCEEDED with available output
    declared_deps = snapshot.task_definition.dependencies
    for dep_id in declared_deps:
        dep_state = snapshot.upstream_dependencies.get(dep_id)
        if dep_state is None:
            return ReadinessDecision(
                ready=False,
                resolved_input=None,
                reason=ReadinessReason.DEPENDENCIES_INCOMPLETE,
                diagnostic_message=f"Direct dependency '{dep_id.value}' has no state recorded.",
            )
        if dep_state.state != "SUCCEEDED":
            return ReadinessDecision(
                ready=False,
                resolved_input=None,
                reason=ReadinessReason.DEPENDENCY_NOT_SUCCEEDED,
                diagnostic_message=f"Dependency '{dep_id.value}' is in state '{dep_state.state}', not 'SUCCEEDED'.",
            )
        if not dep_state.has_output:
            return ReadinessDecision(
                ready=False,
                resolved_input=None,
                reason=ReadinessReason.OUTPUT_NOT_AVAILABLE,
                diagnostic_message=f"Dependency '{dep_id.value}' succeeded but authoritative output is missing.",
            )

    # Resolve named task input bindings (whole-value ADR-010)
    resolved_map: dict[str, JsonValue] = {}
    for param_name, binding in snapshot.task_definition.input_bindings.items():
        if isinstance(binding, LiteralBinding):
            resolved_map[param_name] = binding.value
        elif isinstance(binding, WorkflowInputBinding):
            resolved_map[param_name] = snapshot.workflow_input
        elif isinstance(binding, TaskOutputBinding):
            src_dep = snapshot.upstream_dependencies.get(binding.upstream_task_id)
            if src_dep is None or not src_dep.has_output:
                return ReadinessDecision(
                    ready=False,
                    resolved_input=None,
                    reason=ReadinessReason.INPUT_RESOLUTION_CORRUPT,
                    diagnostic_message=f"TaskOutput source '{binding.upstream_task_id.value}' output unreadable.",
                )
            resolved_map[param_name] = src_dep.output_value
        else:
            return ReadinessDecision(
                ready=False,
                resolved_input=None,
                reason=ReadinessReason.INPUT_RESOLUTION_CORRUPT,
                diagnostic_message=f"Unknown binding variant '{type(binding)}'.",
            )

    return ReadinessDecision(
        ready=True,
        resolved_input=MappingProxyType(resolved_map),
        reason=ReadinessReason.READY,
        diagnostic_message="All direct dependencies succeeded and input resolved successfully.",
    )


def resolve_workflow_outputs(
    output_bindings: Mapping[str, WorkflowTaskOutputBinding],
    task_outputs: Mapping[TaskDefinitionId, JsonValue],
) -> JsonValue:
    """Resolves workflow outputs from task outputs according to LLD-03 Section 8.3 & ADR-010.

    If output_bindings is empty, returns JSON null (None).
    """
    if not output_bindings:
        return None

    result: dict[str, JsonValue] = {}
    for name, binding in output_bindings.items():
        if not isinstance(binding, WorkflowTaskOutputBinding):
            raise TypeError(f"Unsupported workflow output binding type: {type(binding)}")
        if binding.source_task_id not in task_outputs:
            raise KeyError(
                f"Workflow output source task '{binding.source_task_id}' not found in task outputs."
            )
        result[name] = task_outputs[binding.source_task_id]

    return MappingProxyType(result)
