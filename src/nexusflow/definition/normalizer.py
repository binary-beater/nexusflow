"""AST normalizer translating validated DTOs to CandidateWorkflowSpec (LLD-03)."""

from types import MappingProxyType

from nexusflow.definition.dto import (
    LiteralBindingDTO,
    TaskOutputBindingDTO,
    WorkflowDefinitionDTO,
    WorkflowInputBindingDTO,
)
from nexusflow.domain.identifiers import ActivityType, TaskDefinitionId
from nexusflow.domain.json_compat import freeze_json
from nexusflow.domain.spec import (
    CandidateWorkflowSpec,
    InputBinding,
    LiteralBinding,
    TaskDefinition,
    TaskOutputBinding,
    WorkflowInputBinding,
    WorkflowTaskOutputBinding,
)


def normalize_workflow_dto(dto: WorkflowDefinitionDTO) -> CandidateWorkflowSpec:
    """Converts a structurally validated WorkflowDefinitionDTO into a CandidateWorkflowSpec.

    Preserves raw user dependency declarations (including duplicates, self-references,
    empty tasks, and invalid IDs) so the semantic validator can produce accurate diagnostics.
    """
    tasks: list[TaskDefinition] = []
    raw_dependencies: dict[TaskDefinitionId, tuple[str, ...]] = {}

    for task_name, task_dto in dto.tasks.items():
        task_id = TaskDefinitionId(task_name)
        raw_dependencies[task_id] = tuple(task_dto.dependencies)

        # Build domain input bindings
        input_bindings: dict[str, InputBinding] = {}
        for b_name, b_dto in task_dto.input_bindings.items():
            if isinstance(b_dto, LiteralBindingDTO):
                input_bindings[b_name] = LiteralBinding(value=freeze_json(b_dto.value))
            elif isinstance(b_dto, WorkflowInputBindingDTO):
                input_bindings[b_name] = WorkflowInputBinding()
            elif isinstance(b_dto, TaskOutputBindingDTO):
                input_bindings[b_name] = TaskOutputBinding(
                    upstream_task_id=TaskDefinitionId(b_dto.task)
                )

        tasks.append(
            TaskDefinition(
                id=task_id,
                activity_type=ActivityType(task_dto.activity_type),
                dependencies=frozenset(TaskDefinitionId(d) for d in task_dto.dependencies),
                input_bindings=MappingProxyType(input_bindings),
                max_attempts=task_dto.max_attempts,
            )
        )

    output_bindings: dict[str, WorkflowTaskOutputBinding] = {}
    for out_name, out_dto in dto.output_bindings.items():
        output_bindings[out_name] = WorkflowTaskOutputBinding(
            source_task_id=TaskDefinitionId(out_dto.task)
        )

    return CandidateWorkflowSpec(
        workflow_name=dto.workflow_name,
        tasks=tuple(tasks),
        raw_dependencies=MappingProxyType(raw_dependencies),
        output_bindings=MappingProxyType(output_bindings),
    )
