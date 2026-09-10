from nexusflow.definition.codec import serialize_validated_spec
from nexusflow.definition.dto import WorkflowDefinitionDTO
from nexusflow.definition.fingerprint import compute_registration_fingerprint
from nexusflow.definition.normalizer import normalize_workflow_dto
from nexusflow.definition.parser import parse_yaml_to_ast
from nexusflow.definition.validator import SemanticValidator


def test_fingerprint_invariance_to_whitespace_and_comments():
    yaml_1 = """
# Header comment
workflow_name: example

tasks:
  task_a:
    activity_type: demo.act
    dependencies: []
    max_attempts: 2
"""

    yaml_2 = """
workflow_name: "example"
tasks:
  task_a:
    activity_type: "demo.act"
    dependencies: []
    max_attempts: 2

# Trailing comment
"""
    res_1 = SemanticValidator().validate(
        normalize_workflow_dto(WorkflowDefinitionDTO.model_validate(parse_yaml_to_ast(yaml_1)))
    )
    res_2 = SemanticValidator().validate(
        normalize_workflow_dto(WorkflowDefinitionDTO.model_validate(parse_yaml_to_ast(yaml_2)))
    )
    assert res_1.success is not None
    assert res_2.success is not None

    spec_1 = res_1.success.spec
    spec_2 = res_2.success.spec

    fp_1 = compute_registration_fingerprint(serialize_validated_spec(spec_1))
    fp_2 = compute_registration_fingerprint(serialize_validated_spec(spec_2))

    assert fp_1.digest == fp_2.digest


def test_fingerprint_changes_on_semantic_difference():
    yaml_1 = """
workflow_name: example
tasks:
  task_a:
    activity_type: demo.act
    dependencies: []
    max_attempts: 2
"""

    yaml_2 = """
workflow_name: example
tasks:
  task_a:
    activity_type: demo.act
    dependencies: []
    max_attempts: 3
"""
    res_1 = SemanticValidator().validate(
        normalize_workflow_dto(WorkflowDefinitionDTO.model_validate(parse_yaml_to_ast(yaml_1)))
    )
    res_2 = SemanticValidator().validate(
        normalize_workflow_dto(WorkflowDefinitionDTO.model_validate(parse_yaml_to_ast(yaml_2)))
    )
    assert res_1.success is not None
    assert res_2.success is not None

    spec_1 = res_1.success.spec
    spec_2 = res_2.success.spec

    fp_1 = compute_registration_fingerprint(serialize_validated_spec(spec_1))
    fp_2 = compute_registration_fingerprint(serialize_validated_spec(spec_2))

    assert fp_1.digest != fp_2.digest
