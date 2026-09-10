from nexusflow.definition.dto import WorkflowDefinitionDTO
from nexusflow.definition.normalizer import normalize_workflow_dto
from nexusflow.definition.validator import DefinitionValidationCode, SemanticValidator


def test_empty_workflow_rejected():
    dto = WorkflowDefinitionDTO(workflow_name="empty", tasks={}, output_bindings={})
    candidate = normalize_workflow_dto(dto)
    validator = SemanticValidator()
    outcome = validator.validate(candidate)
    assert outcome.success is None
    assert len(outcome.errors) == 1
    assert outcome.errors[0].code == DefinitionValidationCode.EMPTY_WORKFLOW


def test_self_dependency_rejected():
    dto = WorkflowDefinitionDTO.model_validate(
        {
            "workflow_name": "self_dep",
            "tasks": {
                "task_a": {
                    "activity_type": "test.act",
                    "dependencies": ["task_a"],
                    "max_attempts": 1,
                }
            },
        }
    )
    candidate = normalize_workflow_dto(dto)
    outcome = SemanticValidator().validate(candidate)
    assert outcome.success is None
    assert any(e.code == DefinitionValidationCode.SELF_DEPENDENCY for e in outcome.errors)


def test_duplicate_dependency_rejected():
    dto = WorkflowDefinitionDTO.model_validate(
        {
            "workflow_name": "dup_dep",
            "tasks": {
                "task_a": {"activity_type": "test.act", "dependencies": [], "max_attempts": 1},
                "task_b": {
                    "activity_type": "test.act",
                    "dependencies": ["task_a", "task_a"],
                    "max_attempts": 1,
                },
            },
        }
    )
    candidate = normalize_workflow_dto(dto)
    outcome = SemanticValidator().validate(candidate)
    assert outcome.success is None
    assert any(e.code == DefinitionValidationCode.DUPLICATE_DEPENDENCY for e in outcome.errors)


def test_unknown_dependency_rejected():
    dto = WorkflowDefinitionDTO.model_validate(
        {
            "workflow_name": "unk_dep",
            "tasks": {
                "task_a": {
                    "activity_type": "test.act",
                    "dependencies": ["non_existent"],
                    "max_attempts": 1,
                }
            },
        }
    )
    candidate = normalize_workflow_dto(dto)
    outcome = SemanticValidator().validate(candidate)
    assert outcome.success is None
    assert any(e.code == DefinitionValidationCode.UNKNOWN_DEPENDENCY for e in outcome.errors)


def test_task_output_must_reference_direct_dependency():
    dto = WorkflowDefinitionDTO.model_validate(
        {
            "workflow_name": "output_binding_test",
            "tasks": {
                "task_a": {"activity_type": "test.act", "dependencies": [], "max_attempts": 1},
                "task_b": {
                    "activity_type": "test.act",
                    "dependencies": [],  # does not declare task_a as dependency!
                    "input_bindings": {"val": {"type": "task_output", "task": "task_a"}},
                    "max_attempts": 1,
                },
            },
        }
    )
    candidate = normalize_workflow_dto(dto)
    outcome = SemanticValidator().validate(candidate)
    assert outcome.success is None
    assert any(
        e.code == DefinitionValidationCode.TASK_OUTPUT_SOURCE_NOT_DEPENDENCY for e in outcome.errors
    )


def test_workflow_output_source_must_exist():
    dto = WorkflowDefinitionDTO.model_validate(
        {
            "workflow_name": "wf_output_test",
            "tasks": {
                "task_a": {"activity_type": "test.act", "dependencies": [], "max_attempts": 1}
            },
            "output_bindings": {"out": {"type": "task_output", "task": "unknown_task"}},
        }
    )
    candidate = normalize_workflow_dto(dto)
    outcome = SemanticValidator().validate(candidate)
    assert outcome.success is None
    assert any(
        e.code == DefinitionValidationCode.UNKNOWN_WORKFLOW_OUTPUT_SOURCE for e in outcome.errors
    )


def test_cycle_rejected():
    dto = WorkflowDefinitionDTO.model_validate(
        {
            "workflow_name": "cycle_test",
            "tasks": {
                "task_a": {
                    "activity_type": "test.act",
                    "dependencies": ["task_b"],
                    "max_attempts": 1,
                },
                "task_b": {
                    "activity_type": "test.act",
                    "dependencies": ["task_a"],
                    "max_attempts": 1,
                },
            },
        }
    )
    candidate = normalize_workflow_dto(dto)
    outcome = SemanticValidator().validate(candidate)
    assert outcome.success is None
    assert any(e.code == DefinitionValidationCode.CYCLE_DETECTED for e in outcome.errors)


def test_disconnected_dag_accepted():
    dto = WorkflowDefinitionDTO.model_validate(
        {
            "workflow_name": "disconnected_dag",
            "tasks": {
                "island_1": {"activity_type": "test.act", "dependencies": [], "max_attempts": 1},
                "island_2": {"activity_type": "test.act", "dependencies": [], "max_attempts": 1},
            },
        }
    )
    candidate = normalize_workflow_dto(dto)
    outcome = SemanticValidator().validate(candidate)
    assert outcome.success is not None
    assert len(outcome.errors) == 0
    assert len(outcome.success.graph.nodes) == 2


def test_multiple_roots_and_leaves_accepted():
    dto = WorkflowDefinitionDTO.model_validate(
        {
            "workflow_name": "multi_root_leaf",
            "tasks": {
                "root_1": {"activity_type": "test.act", "dependencies": [], "max_attempts": 1},
                "root_2": {"activity_type": "test.act", "dependencies": [], "max_attempts": 1},
                "middle": {
                    "activity_type": "test.act",
                    "dependencies": ["root_1", "root_2"],
                    "max_attempts": 1,
                },
                "leaf_1": {
                    "activity_type": "test.act",
                    "dependencies": ["middle"],
                    "max_attempts": 1,
                },
                "leaf_2": {
                    "activity_type": "test.act",
                    "dependencies": ["middle"],
                    "max_attempts": 1,
                },
            },
        }
    )
    candidate = normalize_workflow_dto(dto)
    outcome = SemanticValidator().validate(candidate)
    assert outcome.success is not None
    assert len(outcome.errors) == 0
    assert len(outcome.success.graph.nodes) == 5
