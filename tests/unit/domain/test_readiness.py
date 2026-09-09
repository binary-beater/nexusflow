"""Unit tests for pure task readiness evaluator and workflow output resolution."""

from types import MappingProxyType

from nexusflow.domain.identifiers import (
    ActivityType,
    TaskDefinitionId,
    TaskExecutionId,
    WorkflowExecutionId,
)
from nexusflow.domain.readiness import (
    DependencyStateSnapshot,
    ReadinessReason,
    TaskReadinessSnapshot,
    evaluate_task_readiness,
    resolve_workflow_outputs,
)
from nexusflow.domain.spec import (
    LiteralBinding,
    TaskDefinition,
    TaskOutputBinding,
    WorkflowInputBinding,
    WorkflowTaskOutputBinding,
)


def test_root_task_readiness():
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    t_def_id = TaskDefinitionId("root_task")

    task_def = TaskDefinition(
        id=t_def_id,
        activity_type=ActivityType("test.root"),
        dependencies=frozenset(),
        input_bindings={"order_id": WorkflowInputBinding()},
        max_attempts=3,
    )

    snapshot = TaskReadinessSnapshot(
        workflow_id=wf_id,
        workflow_state="RUNNING",
        workflow_input={"order_id": "ord_123"},
        task_id=t_id,
        task_definition_id=t_def_id,
        task_state="PENDING",
        task_revision=1,
        task_definition=task_def,
        upstream_dependencies={},
    )

    decision = evaluate_task_readiness(snapshot)
    assert decision.ready is True
    assert decision.reason == ReadinessReason.READY
    assert decision.resolved_input == {"order_id": {"order_id": "ord_123"}}


def test_downstream_task_blocked_until_dependency_succeeds():
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    dep_id = TaskDefinitionId("upstream_task")
    t_def_id = TaskDefinitionId("downstream_task")

    task_def = TaskDefinition(
        id=t_def_id,
        activity_type=ActivityType("test.downstream"),
        dependencies=frozenset([dep_id]),
        input_bindings={
            "upstream_result": TaskOutputBinding(upstream_task_id=dep_id),
            "multiplier": LiteralBinding(value=42),
        },
        max_attempts=3,
    )

    # 1. Blocked when dependency is still RUNNING
    snapshot_pending = TaskReadinessSnapshot(
        workflow_id=wf_id,
        workflow_state="RUNNING",
        workflow_input=None,
        task_id=t_id,
        task_definition_id=t_def_id,
        task_state="PENDING",
        task_revision=1,
        task_definition=task_def,
        upstream_dependencies={
            dep_id: DependencyStateSnapshot(
                task_definition_id=dep_id,
                state="RUNNING",
                has_output=False,
                output_value=None,
            )
        },
    )
    decision_pending = evaluate_task_readiness(snapshot_pending)
    assert decision_pending.ready is False
    assert decision_pending.reason == ReadinessReason.DEPENDENCY_NOT_SUCCEEDED

    # 2. Ready when dependency SUCCEEDED with output
    snapshot_ready = TaskReadinessSnapshot(
        workflow_id=wf_id,
        workflow_state="RUNNING",
        workflow_input=None,
        task_id=t_id,
        task_definition_id=t_def_id,
        task_state="PENDING",
        task_revision=1,
        task_definition=task_def,
        upstream_dependencies={
            dep_id: DependencyStateSnapshot(
                task_definition_id=dep_id,
                state="SUCCEEDED",
                has_output=True,
                output_value={"score": 100},
            )
        },
    )
    decision_ready = evaluate_task_readiness(snapshot_ready)
    assert decision_ready.ready is True
    assert decision_ready.reason == ReadinessReason.READY
    assert decision_ready.resolved_input == {
        "upstream_result": {"score": 100},
        "multiplier": 42,
    }


def test_resolve_workflow_outputs():
    t_a = TaskDefinitionId("task_a")
    t_b = TaskDefinitionId("task_b")

    output_bindings = {
        "final_a": WorkflowTaskOutputBinding(source_task_id=t_a),
        "final_b": WorkflowTaskOutputBinding(source_task_id=t_b),
    }

    task_outputs = {
        t_a: "result_a",
        t_b: {"code": 200},
    }

    result = resolve_workflow_outputs(output_bindings, task_outputs)
    assert isinstance(result, MappingProxyType)
    assert dict(result) == {"final_a": "result_a", "final_b": {"code": 200}}
