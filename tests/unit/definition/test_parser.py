import pytest
from pydantic import ValidationError

from nexusflow.definition.dto import WorkflowDefinitionDTO
from nexusflow.definition.parser import (
    YamlComplexityError,
    YamlSyntaxError,
    parse_yaml_to_ast,
)


def test_valid_yaml_parsing():
    yaml_text = """
workflow_name: test_wf
tasks:
  task_a:
    activity_type: test.act
    dependencies: []
    input_bindings:
      x:
        type: literal
        value: 123
    max_attempts: 2
output_bindings: {}
"""
    ast = parse_yaml_to_ast(yaml_text)
    dto = WorkflowDefinitionDTO.model_validate(ast)
    assert dto.workflow_name == "test_wf"
    assert "task_a" in dto.tasks
    assert dto.tasks["task_a"].max_attempts == 2


def test_unknown_keys_rejected():
    yaml_text = """
workflow_name: test_wf
unknown_key: prohibited
tasks: {}
"""
    ast = parse_yaml_to_ast(yaml_text)
    with pytest.raises(ValidationError):
        WorkflowDefinitionDTO.model_validate(ast)


def test_duplicate_mapping_keys_rejected():
    yaml_text = """
workflow_name: test_wf
tasks: {}
workflow_name: duplicate
"""
    with pytest.raises(YamlSyntaxError):
        parse_yaml_to_ast(yaml_text)


def test_anchors_and_aliases_rejected():
    yaml_text = """
base_task: &base
  activity_type: demo.act
  max_attempts: 1
tasks:
  child: *base
"""
    with pytest.raises(YamlComplexityError, match="anchors and aliases are prohibited"):
        parse_yaml_to_ast(yaml_text)


def test_depth_limit_enforced():
    nested = "val: 1"
    for i in range(10):
        nested = f"layer_{i}:\n  " + nested.replace("\n", "\n  ")
    with pytest.raises(YamlComplexityError, match="nesting depth"):
        parse_yaml_to_ast(nested, max_depth=5)


def test_payload_size_limit_enforced():
    yaml_text = "workflow_name: " + "a" * 200
    with pytest.raises(YamlComplexityError, match="payload size"):
        parse_yaml_to_ast(yaml_text, max_bytes=50)


def test_node_count_limit_enforced():
    yaml_text = "workflow_name: test\n" + "\n".join(f"k{i}: v{i}" for i in range(100))
    with pytest.raises(YamlComplexityError, match="node count"):
        parse_yaml_to_ast(yaml_text, max_nodes=20)


def test_non_mapping_root_rejected():
    with pytest.raises(YamlSyntaxError, match="Root YAML element must be a mapping"):
        parse_yaml_to_ast("- item1\n- item2")


def test_multiple_documents_rejected():
    with pytest.raises(YamlSyntaxError):
        parse_yaml_to_ast("workflow_name: doc1\n---\nworkflow_name: doc2")


def test_binding_discriminators_and_workflow_output_rejections():
    # Valid bindings test
    valid_yaml = """
workflow_name: binding_demo
tasks:
  producer:
    activity_type: demo.produce
    dependencies: []
    input_bindings:
      lit:
        type: literal
        value: "hello"
      inp:
        type: workflow_input
    max_attempts: 1
  consumer:
    activity_type: demo.consume
    dependencies: [producer]
    input_bindings:
      prev:
        type: task_output
        task: producer
    max_attempts: 1
output_bindings:
  res:
    type: task_output
    task: consumer
"""
    ast = parse_yaml_to_ast(valid_yaml)
    dto = WorkflowDefinitionDTO.model_validate(ast)
    assert dto.tasks["producer"].input_bindings["lit"].type == "literal"
    assert dto.tasks["producer"].input_bindings["inp"].type == "workflow_input"
    assert dto.tasks["consumer"].input_bindings["prev"].type == "task_output"
    assert dto.output_bindings["res"].type == "task_output"

    # Reject invalid binding discriminator
    invalid_discriminator = """
workflow_name: bad_disc
tasks:
  step:
    activity_type: demo.act
    dependencies: []
    input_bindings:
      param:
        type: json_path
        path: "$.data"
    max_attempts: 1
"""
    with pytest.raises(ValidationError):
        WorkflowDefinitionDTO.model_validate(parse_yaml_to_ast(invalid_discriminator))

    # Reject workflow output with literal
    invalid_wf_output_literal = """
workflow_name: bad_out
tasks:
  step:
    activity_type: demo.act
    dependencies: []
    max_attempts: 1
output_bindings:
  out:
    type: literal
    value: 123
"""
    with pytest.raises(ValidationError):
        WorkflowDefinitionDTO.model_validate(parse_yaml_to_ast(invalid_wf_output_literal))

    # Reject workflow output with workflow_input passthrough
    invalid_wf_output_input = """
workflow_name: bad_out
tasks:
  step:
    activity_type: demo.act
    dependencies: []
    max_attempts: 1
output_bindings:
  out:
    type: workflow_input
"""
    with pytest.raises(ValidationError):
        WorkflowDefinitionDTO.model_validate(parse_yaml_to_ast(invalid_wf_output_input))
