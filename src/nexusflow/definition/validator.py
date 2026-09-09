"""Pure domain semantic validator implementing ADR-004 and LLD-03 Section 9."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

from nexusflow.domain.graph import CanonicalGraph, build_canonical_graph_and_verify_acyclic
from nexusflow.domain.identifiers import TaskDefinitionId
from nexusflow.domain.json_compat import JsonValue
from nexusflow.domain.spec import (
    CandidateWorkflowSpec,
    TaskOutputBinding,
    ValidatedWorkflowSpec,
)

type JsonObject = Mapping[str, JsonValue]


class DefinitionValidationCode(StrEnum):
    # Structural Errors (ADR-002)
    PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"
    YAML_COMPLEXITY_EXCEEDED = "YAML_COMPLEXITY_EXCEEDED"
    INVALID_YAML = "INVALID_YAML"
    DUPLICATE_KEY = "DUPLICATE_KEY"
    INVALID_ROOT = "INVALID_ROOT"
    MISSING_FIELD = "MISSING_FIELD"
    UNKNOWN_FIELD = "UNKNOWN_FIELD"
    INVALID_FIELD_TYPE = "INVALID_FIELD_TYPE"
    INVALID_BINDING = "INVALID_BINDING"

    # Semantic Errors (ADR-004)
    EMPTY_WORKFLOW = "EMPTY_WORKFLOW"
    UNKNOWN_DEPENDENCY = "UNKNOWN_DEPENDENCY"
    SELF_DEPENDENCY = "SELF_DEPENDENCY"
    DUPLICATE_DEPENDENCY = "DUPLICATE_DEPENDENCY"
    UNKNOWN_TASK_OUTPUT_SOURCE = "UNKNOWN_TASK_OUTPUT_SOURCE"
    TASK_OUTPUT_SOURCE_NOT_DEPENDENCY = "TASK_OUTPUT_SOURCE_NOT_DEPENDENCY"
    UNKNOWN_WORKFLOW_OUTPUT_SOURCE = "UNKNOWN_WORKFLOW_OUTPUT_SOURCE"
    CYCLE_DETECTED = "CYCLE_DETECTED"
    ATTEMPTS_EXCEEDED_LIMIT = "ATTEMPTS_EXCEEDED_LIMIT"


@dataclass(frozen=True, slots=True)
class DefinitionValidationError:
    code: DefinitionValidationCode
    message: str
    path: str
    details: JsonObject | None = None


@dataclass(frozen=True, slots=True)
class ValidatedDefinitionResult:
    spec: ValidatedWorkflowSpec
    graph: CanonicalGraph


@dataclass(frozen=True, slots=True)
class ValidationOutcome:
    success: ValidatedDefinitionResult | None
    errors: tuple[DefinitionValidationError, ...]


class SemanticValidator:
    """Pure in-memory semantic validator implementing ADR-004.

    Accumulates errors across non-dependent checks up to max_errors.
    """

    def __init__(
        self,
        max_attempts_admission_limit: int = 100,
        max_errors: int = 50,
    ) -> None:
        self._max_attempts_admission_limit = max_attempts_admission_limit
        self._max_errors = max_errors

    def validate(self, candidate: CandidateWorkflowSpec) -> ValidationOutcome:
        errors: list[DefinitionValidationError] = []

        def add_error(code: DefinitionValidationCode, msg: str, path: str) -> bool:
            errors.append(DefinitionValidationError(code=code, message=msg, path=path))
            return len(errors) >= self._max_errors

        # Rule 1: Empty workflow check (ADR-004 semantic requirement)
        if not candidate.tasks:
            add_error(
                DefinitionValidationCode.EMPTY_WORKFLOW,
                "Workflow specification must contain at least one task definition.",
                "tasks",
            )
            return ValidationOutcome(success=None, errors=tuple(errors))

        # Build task ID index in O(V)
        task_id_set = {t.id for t in candidate.tasks}
        task_map = {t.id: t for t in candidate.tasks}

        # Rule 2: Dependency integrity checks
        has_reference_errors = False
        for task in candidate.tasks:
            path_prefix = f"tasks.{task.id.value}"

            # Operational admission check for max_attempts
            if task.max_attempts > self._max_attempts_admission_limit:
                if add_error(
                    DefinitionValidationCode.ATTEMPTS_EXCEEDED_LIMIT,
                    f"max_attempts ({task.max_attempts}) exceeds configured admission limit of {self._max_attempts_admission_limit}.",
                    f"{path_prefix}.max_attempts",
                ):
                    return ValidationOutcome(success=None, errors=tuple(errors))

            # Raw dependency inspections
            raw_deps = candidate.raw_dependencies.get(task.id, ())
            seen_deps: set[str] = set()
            for idx, dep_str in enumerate(raw_deps):
                dep_path = f"{path_prefix}.dependencies[{idx}]"
                if dep_str in seen_deps:
                    if add_error(
                        DefinitionValidationCode.DUPLICATE_DEPENDENCY,
                        f"Duplicate dependency '{dep_str}' declared on task '{task.id.value}'.",
                        dep_path,
                    ):
                        return ValidationOutcome(success=None, errors=tuple(errors))
                seen_deps.add(dep_str)

                if dep_str == task.id.value:
                    if add_error(
                        DefinitionValidationCode.SELF_DEPENDENCY,
                        f"Task '{task.id.value}' cannot declare a self-dependency.",
                        dep_path,
                    ):
                        return ValidationOutcome(success=None, errors=tuple(errors))

                dep_id = TaskDefinitionId(dep_str)
                if dep_id not in task_id_set:
                    has_reference_errors = True
                    if add_error(
                        DefinitionValidationCode.UNKNOWN_DEPENDENCY,
                        f"Task '{task.id.value}' references unknown dependency '{dep_str}'.",
                        dep_path,
                    ):
                        return ValidationOutcome(success=None, errors=tuple(errors))

            # Rule 3: Input binding checks
            for input_name, binding in task.input_bindings.items():
                binding_path = f"{path_prefix}.input_bindings.{input_name}"
                if isinstance(binding, TaskOutputBinding):
                    upstream_id = binding.upstream_task_id
                    if upstream_id not in task_id_set:
                        has_reference_errors = True
                        if add_error(
                            DefinitionValidationCode.UNKNOWN_TASK_OUTPUT_SOURCE,
                            f"TaskOutput binding references unknown task '{upstream_id.value}'.",
                            binding_path,
                        ):
                            return ValidationOutcome(success=None, errors=tuple(errors))
                    elif upstream_id not in task.dependencies:
                        if add_error(
                            DefinitionValidationCode.TASK_OUTPUT_SOURCE_NOT_DEPENDENCY,
                            f"TaskOutput source '{upstream_id.value}' must be a direct declared dependency of '{task.id.value}'.",
                            binding_path,
                        ):
                            return ValidationOutcome(success=None, errors=tuple(errors))

        # Rule 4: Workflow output binding checks
        for out_name, out_binding in candidate.output_bindings.items():
            out_path = f"output_bindings.{out_name}"
            src_id = out_binding.source_task_id
            if src_id not in task_id_set:
                if add_error(
                    DefinitionValidationCode.UNKNOWN_WORKFLOW_OUTPUT_SOURCE,
                    f"Workflow output binding references unknown source task '{src_id.value}'.",
                    out_path,
                ):
                    return ValidationOutcome(success=None, errors=tuple(errors))

        # Stop before cycle detection if reference integrity failed
        if has_reference_errors or errors:
            return ValidationOutcome(success=None, errors=tuple(errors))

        # Rule 5: Graph construction and Kahn cycle detection strictly in O(V+E)
        graph, cycle_errors = build_canonical_graph_and_verify_acyclic(task_map)
        if cycle_errors:
            for c_err in cycle_errors:
                add_error(DefinitionValidationCode.CYCLE_DETECTED, c_err, "tasks")
            return ValidationOutcome(success=None, errors=tuple(errors))

        # Rule 6: Promotion to ValidatedWorkflowSpec
        validated_spec = ValidatedWorkflowSpec(
            workflow_name=candidate.workflow_name,
            tasks=MappingProxyType(task_map),
            output_bindings=MappingProxyType(dict(candidate.output_bindings)),
        )

        return ValidationOutcome(
            success=ValidatedDefinitionResult(
                spec=validated_spec,
                graph=graph,
            ),
            errors=(),
        )
