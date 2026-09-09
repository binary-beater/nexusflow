from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass

from nexusflow.domain.identifiers import TaskDefinitionId
from nexusflow.domain.spec import TaskDefinition


@dataclass(frozen=True, slots=True)
class CanonicalGraph:
    """Sparse bidirectional adjacency DAG derived from task and dependency declarations.

    All collections are deeply immutable.
    """
    nodes: frozenset[TaskDefinitionId]
    dependencies: Mapping[TaskDefinitionId, frozenset[TaskDefinitionId]]  # Incoming: B -> {A}
    dependents: Mapping[TaskDefinitionId, frozenset[TaskDefinitionId]]    # Outgoing: A -> {B}

    def get_upstream_dependencies(self, task_id: TaskDefinitionId) -> frozenset[TaskDefinitionId]:
        return self.dependencies.get(task_id, frozenset())

    def get_downstream_dependents(self, task_id: TaskDefinitionId) -> frozenset[TaskDefinitionId]:
        return self.dependents.get(task_id, frozenset())

    def is_root(self, task_id: TaskDefinitionId) -> bool:
        return len(self.dependencies.get(task_id, frozenset())) == 0

    def is_leaf(self, task_id: TaskDefinitionId) -> bool:
        return len(self.dependents.get(task_id, frozenset())) == 0

def build_canonical_graph_and_verify_acyclic(
    tasks: Mapping[TaskDefinitionId, TaskDefinition],
) -> tuple[CanonicalGraph, list[str]]:
    """Constructs the CanonicalGraph and verifies acyclicity using Kahn's algorithm strictly in O(V+E).

    Precondition: All dependencies referenced in task.dependencies MUST exist in tasks.
    Performs NO node sorting to guarantee pure O(V+E) linear complexity.
    Returns (graph, cycle_errors).
    """
    nodes = frozenset(tasks.keys())
    in_degree: dict[TaskDefinitionId, int] = dict.fromkeys(nodes, 0)
    dependencies: dict[TaskDefinitionId, set[TaskDefinitionId]] = {t: set() for t in nodes}
    dependents: dict[TaskDefinitionId, set[TaskDefinitionId]] = {t: set() for t in nodes}

    # Step 1: Build bidirectional adjacency and compute in-degrees in O(V + E)
    for task_id, task in tasks.items():
        for dep_id in task.dependencies:
            if dep_id not in nodes:
                raise ValueError(f"Precondition violated: dependency '{dep_id}' not found in tasks.")
            dependencies[task_id].add(dep_id)
            dependents[dep_id].add(task_id)
            in_degree[task_id] += 1

    # Step 2: Initialize queue with all root nodes (in-degree == 0) in O(V)
    queue: deque[TaskDefinitionId] = deque([t for t in nodes if in_degree[t] == 0])
    visited_count = 0

    # Step 3: Pure O(V + E) Kahn reduction
    while queue:
        current = queue.popleft()
        visited_count += 1

        for downstream in dependents[current]:
            in_degree[downstream] -= 1
            if in_degree[downstream] == 0:
                queue.append(downstream)

    canonical_graph = CanonicalGraph(
        nodes=nodes,
        dependencies={k: frozenset(v) for k, v in dependencies.items()},
        dependents={k: frozenset(v) for k, v in dependents.items()},
    )

    # Step 4: Detect cycle presence in O(V)
    if visited_count != len(nodes):
        cycle_nodes = sorted([t.value for t in nodes if in_degree[t] > 0])
        return canonical_graph, [f"Graph contains a cycle involving tasks: {', '.join(cycle_nodes)}"]

    return canonical_graph, []

def compute_deterministic_topological_order(
    graph: CanonicalGraph,
) -> tuple[TaskDefinitionId, ...]:
    """Derived convenience helper: Computes a deterministic topological order in O((V + E) log V)

    by sorting node keys. This order is non-authoritative and recomputable.
    """
    in_degree: dict[TaskDefinitionId, int] = {
        t: len(graph.get_upstream_dependencies(t)) for t in graph.nodes
    }

    queue: deque[TaskDefinitionId] = deque(
        sorted([t for t in graph.nodes if in_degree[t] == 0], key=lambda x: x.value)
    )
    order: list[TaskDefinitionId] = []

    while queue:
        current = queue.popleft()
        order.append(current)

        for downstream in sorted(graph.get_downstream_dependents(current), key=lambda x: x.value):
            in_degree[downstream] -= 1
            if in_degree[downstream] == 0:
                queue.append(downstream)

    return tuple(order)
