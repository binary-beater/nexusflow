"""Persistence serialization and deserialization codec (LLD-03 Section 11)."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from nexusflow.domain.graph import build_canonical_graph_and_verify_acyclic
from nexusflow.domain.identifiers import ActivityType, TaskDefinitionId
from nexusflow.domain.json_compat import freeze_json, thaw_json
from nexusflow.domain.spec import (
    InputBinding,
    LiteralBinding,
    TaskDefinition,
    TaskOutputBinding,
    ValidatedWorkflowSpec,
    WorkflowInputBinding,
    WorkflowTaskOutputBinding,
)


class PersistenceIntegrityError(Exception):
    """Raised when persisted specification in the database violates integrity invariants."""


def serialize_validated_spec(spec: ValidatedWorkflowSpec) -> dict[str, Any]:
    """Serializes a ValidatedWorkflowSpec into the exact LLD-02 JSONB persistence schema.

    Returns exclusively JSON-native Python dictionaries, lists, and primitives.
    Passes all domain JsonValue instances through thaw_json to guarantee JSON-native output.
    """
    tasks_dict: dict[str, Any] = {}
    for task_id in sorted(spec.tasks.keys(), key=lambda t: t.value):
        task = spec.tasks[task_id]

        input_bindings_dict: dict[str, Any] = {}
        for input_name in sorted(task.input_bindings.keys()):
            binding = task.input_bindings[input_name]
            if isinstance(binding, LiteralBinding):
                input_bindings_dict[input_name] = {
                    "type": "Literal",
                    "value": thaw_json(binding.value),
                }
            elif isinstance(binding, WorkflowInputBinding):
                input_bindings_dict[input_name] = {"type": "WorkflowInput"}
            elif isinstance(binding, TaskOutputBinding):
                input_bindings_dict[input_name] = {
                    "type": "TaskOutput",
                    "upstream_task_id": binding.upstream_task_id.value,
                }

        tasks_dict[task_id.value] = {
            "id": task_id.value,
            "activity_type": task.activity_type.name,
            "dependencies": sorted([dep.value for dep in task.dependencies]),
            "input_bindings": input_bindings_dict,
            "max_attempts": task.max_attempts,
        }

    output_bindings_dict: dict[str, Any] = {}
    for out_name in sorted(spec.output_bindings.keys()):
        out_binding = spec.output_bindings[out_name]
        output_bindings_dict[out_name] = {
            "type": "WorkflowTaskOutput",
            "source_task_id": out_binding.source_task_id.value,
        }

    return {
        "workflow_name": spec.workflow_name,
        "tasks": tasks_dict,
        "output_bindings": output_bindings_dict,
    }


def deserialize_validated_spec(data: Any) -> ValidatedWorkflowSpec:
    """Reconstructs an immutable ValidatedWorkflowSpec directly from persisted JSONB.

    Performs fail-closed integrity validation on the durable structure.
    Strictly verifies physical JSON types with zero coercion.
    """
    try:
        if not isinstance(data, (dict, Mapping)):
            raise PersistenceIntegrityError(
                f"Root durable spec must be a mapping, got {type(data).__name__}."
            )

        wf_name = data.get("workflow_name")
        if not isinstance(wf_name, str) or not wf_name:
            raise PersistenceIntegrityError("Durable spec missing valid string 'workflow_name'.")

        raw_tasks = data.get("tasks")
        if not isinstance(raw_tasks, (dict, Mapping)):
            raise PersistenceIntegrityError(
                f"'tasks' must be a mapping, got {type(raw_tasks).__name__}."
            )
        if not raw_tasks:
            raise PersistenceIntegrityError("Durable specification has 0 tasks.")

        raw_outputs = data.get("output_bindings", {})
        if not isinstance(raw_outputs, (dict, Mapping)):
            raise PersistenceIntegrityError(
                f"'output_bindings' must be a mapping, got {type(raw_outputs).__name__}."
            )

        task_id_set: set[TaskDefinitionId] = set()
        for t_key in raw_tasks.keys():
            if not isinstance(t_key, str) or not t_key:
                raise PersistenceIntegrityError(
                    f"Task key must be a non-empty string, got {t_key!r}."
                )
            task_id_set.add(TaskDefinitionId(t_key))

        tasks: dict[TaskDefinitionId, TaskDefinition] = {}
        for t_id_str, t_data in raw_tasks.items():
            if not isinstance(t_data, (dict, Mapping)):
                raise PersistenceIntegrityError(
                    f"Task payload for '{t_id_str}' must be a mapping, got {type(t_data).__name__}."
                )

            t_id = TaskDefinitionId(t_id_str)
            if "id" in t_data:
                if not isinstance(t_data["id"], str) or t_data["id"] != t_id_str:
                    raise PersistenceIntegrityError(
                        f"Task map key '{t_id_str}' mismatches embedded id '{t_data.get('id')}'."
                    )

            raw_act_type = t_data.get("activity_type")
            if not isinstance(raw_act_type, str) or not raw_act_type:
                raise PersistenceIntegrityError(
                    f"Task '{t_id_str}' missing valid string 'activity_type'."
                )
            act_type = ActivityType(raw_act_type)

            raw_deps = t_data.get("dependencies", [])
            if not isinstance(raw_deps, list):
                raise PersistenceIntegrityError(
                    f"Task '{t_id_str}' dependencies must be a list, got {type(raw_deps).__name__}."
                )

            deps_set: set[TaskDefinitionId] = set()
            for d in raw_deps:
                if not isinstance(d, str):
                    raise PersistenceIntegrityError(
                        f"Task '{t_id_str}' dependency item must be string, got {type(d).__name__}."
                    )
                dep_id = TaskDefinitionId(d)
                if dep_id in deps_set:
                    raise PersistenceIntegrityError(
                        f"Task '{t_id_str}' contains duplicate stored dependency '{d}'."
                    )
                if dep_id == t_id:
                    raise PersistenceIntegrityError(
                        f"Task '{t_id_str}' contains stored self-dependency."
                    )
                if dep_id not in task_id_set:
                    raise PersistenceIntegrityError(
                        f"Task '{t_id_str}' references unknown stored dependency '{d}'."
                    )
                deps_set.add(dep_id)

            raw_max_attempts = t_data.get("max_attempts")
            if type(raw_max_attempts) is not int:
                raise PersistenceIntegrityError(
                    f"Task '{t_id_str}' max_attempts must be exact integer, got {type(raw_max_attempts).__name__} ({raw_max_attempts!r})."
                )
            if raw_max_attempts < 1:
                raise PersistenceIntegrityError(
                    f"Task '{t_id_str}' has invalid stored max_attempts {raw_max_attempts} (< 1)."
                )
            max_attempts = raw_max_attempts

            raw_in_bindings = t_data.get("input_bindings", {})
            if not isinstance(raw_in_bindings, (dict, Mapping)):
                raise PersistenceIntegrityError(
                    f"Task '{t_id_str}' input_bindings must be a mapping, got {type(raw_in_bindings).__name__}."
                )

            in_bindings: dict[str, InputBinding] = {}
            for in_k, in_v in raw_in_bindings.items():
                if not isinstance(in_k, str):
                    raise PersistenceIntegrityError(
                        f"Binding key must be string, got {type(in_k).__name__}."
                    )
                if not isinstance(in_v, (dict, Mapping)):
                    raise PersistenceIntegrityError(
                        f"Binding value for '{in_k}' must be a mapping, got {type(in_v).__name__}."
                    )

                b_type = in_v.get("type")
                if not isinstance(b_type, str):
                    raise PersistenceIntegrityError(f"Binding '{in_k}' missing string 'type'.")

                if b_type == "Literal":
                    if "value" not in in_v:
                        raise PersistenceIntegrityError(
                            f"Literal binding '{in_k}' missing 'value'."
                        )
                    in_bindings[in_k] = LiteralBinding(value=freeze_json(in_v["value"]))
                elif b_type == "WorkflowInput":
                    in_bindings[in_k] = WorkflowInputBinding()
                elif b_type == "TaskOutput":
                    upstream_str = in_v.get("upstream_task_id")
                    if not isinstance(upstream_str, str):
                        raise PersistenceIntegrityError(
                            f"TaskOutput binding '{in_k}' missing string 'upstream_task_id'."
                        )
                    upstream_id = TaskDefinitionId(upstream_str)
                    if upstream_id not in task_id_set:
                        raise PersistenceIntegrityError(
                            f"TaskOutput references unknown stored task '{upstream_id.value}'."
                        )
                    if upstream_id not in deps_set:
                        raise PersistenceIntegrityError(
                            f"TaskOutput source '{upstream_id.value}' is not a stored dependency."
                        )
                    in_bindings[in_k] = TaskOutputBinding(upstream_task_id=upstream_id)
                else:
                    raise PersistenceIntegrityError(f"Corrupt stored binding type '{b_type}'.")

            tasks[t_id] = TaskDefinition(
                id=t_id,
                activity_type=act_type,
                dependencies=frozenset(deps_set),
                input_bindings=MappingProxyType(in_bindings),
                max_attempts=max_attempts,
            )

        outputs: dict[str, WorkflowTaskOutputBinding] = {}
        for o_k, o_v in raw_outputs.items():
            if not isinstance(o_k, str):
                raise PersistenceIntegrityError(
                    f"Output binding key must be string, got {type(o_k).__name__}."
                )
            if not isinstance(o_v, (dict, Mapping)):
                raise PersistenceIntegrityError(
                    f"Output binding value for '{o_k}' must be a mapping, got {type(o_v).__name__}."
                )

            ob_type = o_v.get("type")
            if ob_type == "WorkflowTaskOutput":
                src_str = o_v.get("source_task_id")
                if not isinstance(src_str, str):
                    raise PersistenceIntegrityError(
                        f"Output binding '{o_k}' missing string 'source_task_id'."
                    )
                src_id = TaskDefinitionId(src_str)
                if src_id not in task_id_set:
                    raise PersistenceIntegrityError(
                        f"Stored workflow output references unknown task '{src_id.value}'."
                    )
                outputs[o_k] = WorkflowTaskOutputBinding(source_task_id=src_id)
            else:
                raise PersistenceIntegrityError(f"Corrupt stored workflow output type '{ob_type}'.")

        _, cycle_errors = build_canonical_graph_and_verify_acyclic(tasks)
        if cycle_errors:
            raise PersistenceIntegrityError(
                f"Corrupt stored specification contains cycles: {cycle_errors}"
            )

        return ValidatedWorkflowSpec(
            workflow_name=wf_name,
            tasks=MappingProxyType(tasks),
            output_bindings=MappingProxyType(outputs),
        )
    except PersistenceIntegrityError:
        raise
    except Exception as exc:
        raise PersistenceIntegrityError(f"Corrupt durable specification: {exc}") from exc
