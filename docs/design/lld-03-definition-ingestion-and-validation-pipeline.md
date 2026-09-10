# NexusFlow V1 — LLD-03: Definition Ingestion & Validation Pipeline

**Document Status:** Architecture-Ready / Approved as LLD-04 Input  
**Authoritative References:** ADR-001 (Intermediate Workflow Specification), ADR-002 (Definition Parsing), ADR-003 (Canonical Graph Representation), ADR-004 (Definition Validation), ADR-009 (Task Routing), ADR-010 (Workflow Data Flow), ADR-018 (Error Handling), ADR-020 (Technology Selection), ADR-021 (Testing Strategy), ADR-022 (Security), ADR-023 (Configuration), NexusFlow V1 HLD, LLD-01 (Domain Model & Module Contracts), LLD-02 (PostgreSQL Schema & Persistence Transactions).  
**Downstream Dependents:** LLD-04 (Dispatch, Routing & Polling Infrastructure), LLD-08 (External Ingestion HTTP APIs).  

---

## 1. Primary Objective & Architectural Boundary

### 1.1 Objective Statement
This document defines the complete implementation-level design for converting an untrusted, external NexusFlow workflow definition written in YAML into an immutable, semantically validated `ValidatedWorkflowSpec` consumed by persistence (LLD-02) and execution dispatch (LLD-04).

Specifically, it answers:
> *Exactly how does untrusted YAML become a deterministic Candidate Workflow Specification, undergo complete ADR-004 semantic validation, construct its canonical graph, and become the immutable `ValidatedWorkflowSpec` persisted by LLD-02?*

The pipeline enforces a strict physical and conceptual separation between structural parsing (ADR-002) and semantic graph validation (ADR-004):

```
                        [ Untrusted YAML String / Bytes ]
                                       │
                                       ▼ (Size limit check on raw bytes)
                        [ Safe YAML Parsing (ruamel.yaml) ]
                                       │
                                       ▼ (Post-compose Node depth & alias/anchor check)
                           [ External DTO Parsing ]
                                       │ (extra='forbid', explicit fields)
                                       ▼
                         [ CandidateWorkflowSpec ]
                                       │
                      ┌────────────────┴────────────────┐
                      ▼                                 ▼
             (Structural Failures)           [ Semantic Validator ]
                      │                                 │
                      │                      (References, Sets, Direct Bindings)
                      │                                 │
                      │                                 ▼
                      │                     [ Canonical Graph Builder ]
                      │                                 │
                      │                      (O(V+E) Kahn's Cycle Detect)
                      │                                 │
                      │                                 ▼
                      │                     [ ValidatedWorkflowSpec ]
                      │                                 │
                      │                      ┌──────────┴──────────┐
                      │                      ▼                     ▼
                      │             [ Persistence Codec ]   [ Canonical SHA-256 ]
                      │              (thaw_json conversion)        │
                      │                      │                     │
                      ▼                      ▼                     ▼
              [ Structural or        [ LLD-02 validated_iws ] [ RequestFingerprint ]
             Semantic Failures ]     [ registered_defs DB ]
```

### 1.2 Boundary Ownership
* **LLD-03 Owns:**
  - Raw YAML byte/string loading and payload size enforcement.
  - Safe YAML parser configuration via `ruamel.yaml` using pure-Python safe loading mode with explicit Node depth, total count, and anchor/alias rejection.
  - Boundary Data Transfer Objects (DTOs) with strict schema verification and `extra='forbid'`.
  - Allowing structurally valid empty tasks (`tasks: {}`) to produce a `CandidateWorkflowSpec` so ADR-004 semantic validation detects and reports `EMPTY_WORKFLOW`.
  - Enabling `CandidateWorkflowSpec` to represent any semantically invalid graph shape (empty tasks, missing references, self-dependencies, duplicate dependencies, cycles, and illegal output references).
  - Input binding and workflow output binding parsing and representation (restricted strictly to approved V1 whole-value sources).
  - Pure domain semantic validation of candidate definitions (ADR-004).
  - Construction of the sparse bidirectional `CanonicalGraph` in $O(V+E)$ time (ADR-003).
  - Detection and deterministic reporting of cycle errors via Kahn's algorithm in $O(V+E)$ without requiring node sorting.
  - Providing an optional derived deterministic topological ordering for diagnostics or evaluation, clearly documenting its independent complexity ($O((V+E) \log V)$) as non-authoritative.
  - Formal promotion of `CandidateWorkflowSpec` to `ValidatedWorkflowSpec`.
  - Deterministic serialization matching LLD-02's `registered_definitions.validated_iws` JSONB schema, using the frozen LLD-02 `thaw_json` contract to convert frozen domain JSON values into ordinary JSON-native Python dictionaries and lists.
  - Trusted persistence deserialization codec with strict physical type checks and fail-closed integrity validation without coercion.
  - Deterministic request fingerprinting (`RequestFingerprint` SHA-256) for registration idempotency.
  - Typed application outcomes (`CREATED`, `IDEMPOTENT_MATCH`, `STRUCTURAL_VALIDATION_FAILURE`, `SEMANTIC_VALIDATION_FAILURE`, `IDEMPOTENCY_CONFLICT`, `PERSISTENCE_FAILURE`) leaving HTTP status code mapping entirely to LLD-08.
  - Unit, property, and robustness test suites for definition ingestion.

* **LLD-03 Does NOT Own:**
  - Database connection, transaction management, or relational persistence execution (owned by LLD-02).
  - Task execution scheduling, eligibility evaluation, and dispatch state transitions (owned by LLD-04).
  - Worker capability matching, session discovery, or routing candidate selection (owned by LLD-04).
  - Worker heartbeat protocol, task claim delivery, or attempt execution (owned by LLD-05).
  - Execution timeout enforcement, attempt retries, or backoff computation (owned by LLD-06).
  - Engine crash recovery or orchestrator lease management (owned by LLD-07).
  - HTTP routing, TLS termination, or API authentication (owned by LLD-08).

---

## 2. Technology Constraints & Prohibited Patterns

### 2.1 Technology Stack Selection
Conforming strictly to ADR-020:
- **Runtime:** Python 3.12+ with standard type hints (`typing`, `types.MappingProxyType`).
- **YAML Engine:** `ruamel.yaml` configured strictly as a pure-Python safe parser (`typ="safe", pure=True`) with custom constructor hooks disabled and duplicate keys rejected.
- **Boundary Validation:** Pydantic v2 used *strictly* at the external ingestion boundary (`interfaces/definition_ingestion/yaml_models.py`) for initial shape validation.
- **Domain Layer:** Plain frozen dataclasses (`@dataclass(frozen=True, slots=True)`), standard enums, and frozen collections (`frozenset`, `tuple`, `MappingProxyType`).
- **Graph Engine:** Custom, sparse bidirectional adjacency graph implemented directly in pure Python.
- **Testing:** `pytest` with `pytest-asyncio` and `Hypothesis` for property-based DAG generation.

### 2.2 Explicitly Prohibited Technologies & Anti-Patterns
1. **No NetworkX:** Third-party graph libraries are prohibited. All graph operations ($O(V+E)$ traversal, Kahn's cycle detection, adjacency lookups) must be self-contained.
2. **No PyYAML:** PyYAML is prohibited due to historical unsafe loading vulnerabilities, loose YAML 1.1 scalar parsing, and poor duplicate key handling.
3. **No JSON Schema Runtime Engine:** JSON Schema validation libraries (e.g., `jsonschema`) are prohibited as a mandatory runtime dependency.
4. **No Database Dependencies During Validation:** Parsing and semantic validation must be pure in-memory computations without database lookups or connection pool acquisition.
5. **No Worker Registry Coupling:** Definition validation does not verify `ActivityType` against connected workers. Workflows defining activity types not currently serviced by active workers are completely valid.
6. **No Runtime Mutable Flags:** Never use mutable markers such as `validated: bool = False`. Candidate and Validated specifications are distinct, incompatible types.
7. **No Auto-Repair or Coercion:** The parser and validator never silently fix invalid input (e.g., trimming whitespace from IDs, folding case, deduplicating dependency lists, or coercing scalar types).

---

## 3. Trust Boundary & Safe YAML Parsing Configuration

External workflow definition YAML is untrusted input. Prior to reaching any domain logic, it is subjected to strict security boundaries.

### 3.1 Parser Configuration Parameters
The `ruamel.yaml.YAML` parser is configured with absolute restrictions:

```python
from ruamel.yaml import YAML
from ruamel.yaml.nodes import MappingNode, SequenceNode, ScalarNode, Node


def create_safe_yaml_parser() -> YAML:
    """
    Constructs a hardened, pure-Python safe ruamel.yaml parser.
    Disallows executable constructors, custom tags, and duplicate mapping keys.
    """
    yaml = YAML(typ="safe", pure=True)
    yaml.allow_duplicate_keys = False  # Enforces duplicate key rejection at parser level
    return yaml
```

### 3.2 Concrete YAML Complexity & Depth Enforcement
To defend against algorithmic complexity and YAML expansion attacks (e.g., "Billion Laughs" via anchors/aliases), NexusFlow inspects the parser composed Node tree before constructing Python objects.

In `ruamel.yaml`, composed nodes represent YAML anchors via the `anchor` attribute on `Node`, while alias nodes appear as `ruamel.yaml.nodes.AliasNode` (or nodes referencing previously declared anchors). NexusFlow inspects both attributes to reject anchors and aliases completely in V1 definitions:

```python
from ruamel.yaml.nodes import Node, MappingNode, SequenceNode


class YamlComplexityError(Exception):
    """Raised when YAML structure exceeds operational safety thresholds."""

    pass


def enforce_yaml_ast_complexity(
    node: Node,
    max_depth: int,
    max_nodes: int,
    allow_aliases: bool = False,
) -> None:
    """
    Traverses the ruamel.yaml composed Node tree to verify tree depth,
    total node count, and alias/anchor policies before building python objects.
    """
    total_nodes = 0

    def _traverse(current_node: Node, current_depth: int) -> None:
        nonlocal total_nodes
        total_nodes += 1

        if total_nodes > max_nodes:
            raise YamlComplexityError(f"YAML node count exceeds limit of {max_nodes}.")
        if current_depth > max_depth:
            raise YamlComplexityError(f"YAML nesting depth exceeds limit of {max_depth}.")

        # In V1: Reject all anchors and aliases to eliminate expansion vulnerabilities
        if not allow_aliases:
            # Check for anchor declaration (e.g., &base)
            if getattr(current_node, "anchor", None) is not None:
                raise YamlComplexityError(
                    "YAML anchors and aliases are prohibited in workflow definitions."
                )
            # Check for AliasNode type or alias marker (e.g., *base)
            node_type_name = type(current_node).__name__
            if "Alias" in node_type_name or getattr(current_node, "is_alias", False):
                raise YamlComplexityError(
                    "YAML anchors and aliases are prohibited in workflow definitions."
                )

        if isinstance(current_node, MappingNode):
            for key_node, value_node in current_node.value:
                _traverse(key_node, current_depth + 1)
                _traverse(value_node, current_depth + 1)
        elif isinstance(current_node, SequenceNode):
            for item_node in current_node.value:
                _traverse(item_node, current_depth + 1)

    _traverse(node, 1)
```

### 3.3 Parsing Enforcement Rules
1. **Payload Size Limit:** The parser strictly enforces an operational byte limit *before* passing the stream to `ruamel.yaml`. Oversized payloads are immediately rejected before parsing begins.
2. **Post-Compose Complexity Checks:** AST depth, node count, and alias restrictions are enforced on the composed node tree prior to python mapping construction.
3. **Single-Document Enforcement:** Workflows must consist of exactly one YAML document. Multi-document streams (separated by `---`) or documents followed by additional streams are rejected with `INVALID_YAML`.
4. **Non-Empty Mapping Root:** The root of the YAML document must parse as an associative mapping (dictionary). Null documents, scalar roots, or list roots fail immediately with `INVALID_ROOT`.
5. **Tag & Constructor Blacklisting:** Unsafe YAML tags (e.g., `!!python/object`, `!!python/name`, `!custom`) raise parser syntax errors.
6. **Duplicate Key Rejection:** Duplicate keys within any mapping (at root, tasks, dependencies, or bindings) raise a structural error `DUPLICATE_KEY`.

---

## 4. JSON-Compatible Value Semantics & Type Surprises

NexusFlow payload semantics (ADR-010, LLD-01) operate strictly on the logical JSON data model:
$$\text{JsonValue} \in \{\text{null}, \text{boolean}, \text{finite number}, \text{string}, \text{array of JsonValue}, \text{object with string keys and JsonValue}\}$$

Because YAML is a superset of JSON, standard YAML parsers produce Python data structures incompatible with this model.

### 4.1 Treatment of YAML Type Surprises
The table below specifies how scalar types produced by `ruamel.yaml` under `pure=True` safe loading are handled during structural parsing:

| External YAML Token / Syntax | Parsed Python Type | NexusFlow Treatment | Rationale & Invariant |
| :--- | :--- | :--- | :--- |
| `when: 2026-09-07` | `datetime.date` | **REJECT** (`INVALID_FIELD_TYPE`) | Implicit dates violate strict JSON typing. Must be quoted string `"2026-09-07"`. |
| `at: 2026-09-07T12:00:00Z` | `datetime.datetime` | **REJECT** (`INVALID_FIELD_TYPE`) | Implicit timestamps violate strict JSON typing. Must be quoted ISO-8601 string. |
| `val: .nan` | `float('nan')` | **REJECT** (`INVALID_FIELD_TYPE`) | Non-finite numbers violate RFC 8259 JSON and ADR-010. |
| `val: .inf`, `val: -.inf` | `float('inf')` | **REJECT** (`INVALID_FIELD_TYPE`) | Infinities violate RFC 8259 JSON and ADR-010. |
| `data: !!binary "..."` | `bytes` | **REJECT** (`INVALID_FIELD_TYPE`) | Arbitrary binary byte arrays are not supported JSON scalars. Base64 strings required. |
| `10: "integer key"` | `{10: "..."}` | **REJECT** (`INVALID_FIELD_TYPE`) | Mapping keys must be strings. Integer, boolean, or null keys are strictly rejected. |
| `flag: true` / `flag: false` | `bool` | **PERMIT** | Valid standard JSON boolean. |
| `flag: "true"` | `str` | **PERMIT** | Distinct JSON string. Not equivalent to boolean `true`. |
| `flag: yes` / `flag: on` | `str` | **PERMIT (as string)** | In pure safe YAML 1.2 parsing, unquoted `yes` and `on` are parsed as strings, NOT booleans. |
| `val: null` / `val: ~` | `None` | **PERMIT** | Valid JSON null. Distinguishable from absent field. |
| `num: 42`, `num: 3.1415` | `int`, `float` | **PERMIT** | Finite JSON numbers. |

### 4.2 Fingerprint Consequence of Scalar Parsing
Because request fingerprinting occurs *after* structural parsing, scalar interpretation directly influences semantic identity:
- `flag: true` produces JSON boolean `true` in normalized DTOs.
- `flag: "true"` produces JSON string `"true"` in normalized DTOs.
- These representations are semantically distinct and produce different SHA-256 `RequestFingerprint` digests.
- Unquoted YAML 1.2 tokens like `yes` and `on` parse as strings (`"yes"`, `"on"`) and are fingerprinted as strings.

### 4.3 Immutable Domain Canonicalization: `freeze_json`
All literal values embedded in definitions must pass the frozen LLD-01 semantic canonicalization contract:

```python
import math
from types import MappingProxyType
from typing import Mapping, Sequence, Union, TypeAlias

JsonPrimitive: TypeAlias = Union[None, bool, int, float, str]
JsonArray: TypeAlias = tuple["JsonValue", ...]
JsonObject: TypeAlias = Mapping[str, "JsonValue"]
JsonValue: TypeAlias = Union[JsonPrimitive, JsonArray, JsonObject]


def freeze_json(value: object) -> JsonValue:
    """
    Recursively canonicalizes and deeply freezes an arbitrary JSON-compatible structure
    into an immutable domain representation (tuples for arrays, MappingProxyType for objects).
    Rejects NaN, Infinity, bytes, datetimes, and mapping keys that are not strings.
    Never silently stringifies non-string mapping keys.
    """
    if value is None:
        return None
    elif isinstance(value, bool):
        return value
    elif isinstance(value, int):
        return value
    elif isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise ValueError(f"Non-finite JSON numbers (NaN, Infinity) are prohibited: {value}")
        return value
    elif isinstance(value, str):
        return value
    elif isinstance(value, (list, tuple)):
        return tuple(freeze_json(item) for item in value)
    elif isinstance(value, (dict, Mapping)):
        frozen_map: dict[str, JsonValue] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise TypeError(
                    f"Illegal non-string mapping key '{k}' of type '{type(k).__name__}'."
                )
            frozen_map[k] = freeze_json(v)
        return MappingProxyType(frozen_map)
    else:
        raise TypeError(
            f"Object of type '{type(value).__name__}' is not JSON-compatible: {value!r}"
        )
```

### 4.4 Thawing Domain JSON: `thaw_json`
To serialize frozen domain JSON into PostgreSQL persistence, NexusFlow reuses the frozen LLD-02 `thaw_json` contract:

```python
from typing import Any


def thaw_json(val: Any) -> Any:
    """
    Recursively converts frozen domain JSON (tuples, MappingProxyType) into
    ordinary mutable JSON-native Python structures (list, dict with string keys).

    Fails closed: strictly rejects non-string keys and non-finite floats (NaN/Infinity).
    Never converts non-string keys to strings via str(k).
    """
    if val is None or isinstance(val, (int, str, bool)):
        return val
    if isinstance(val, float):
        if not math.isfinite(val):
            raise ValueError(f"Non-finite float value {val} is not valid JSON")
        return val
    if isinstance(val, tuple):
        return [thaw_json(x) for x in val]
    if isinstance(val, (MappingProxyType, dict)):
        result = {}
        for k, v in val.items():
            if not isinstance(k, str):
                raise TypeError(f"JSON object keys must be strings, found: {type(k).__name__}")
            result[k] = thaw_json(v)
        return result
    raise TypeError(f"Unsupported domain JSON type for thawing: {type(val)}")
```

---

## 5. Structural vs. Semantic Validation Boundary

To guarantee diagnostic clarity and system robustness, definition ingestion establishes an unbridgeable boundary between **Structural/Parsing** checks (ADR-002) and **Semantic/Graph** checks (ADR-004).

```
   ┌────────────────────────────────────────────────────────────────────────┐
   │                       PHASE 1: STRUCTURAL PARSING                      │
   │  Operates on untrusted YAML text / DTOs. Asserts syntax, types,        │
   │  field presence, and schema bounds. Rejects unknown fields.            │
   │  Allows tasks: {} to produce CandidateWorkflowSpec.                    │
   │  Outcome: CandidateWorkflowSpec OR Structural Validation Errors        │
   └───────────────────────────────────┬────────────────────────────────────┘
                                       │
                                       ▼
   ┌────────────────────────────────────────────────────────────────────────┐
   │                       PHASE 2: SEMANTIC VALIDATION                     │
   │  Operates on CandidateWorkflowSpec. Evaluates empty task set,          │
   │  missing references, self-dependencies, duplicate dependencies,        │
   │  invalid TaskOutput sources, and cycles.                               │
   │  Outcome: ValidatedWorkflowSpec + CanonicalGraph OR Semantic Errors    │
   └────────────────────────────────────────────────────────────────────────┘
```

### 5.1 Structural Phase Invariants (ADR-002)
- Focuses entirely on syntax, schema conformity, scalar types, and object shapes.
- Rejects malformed YAML, non-mapping roots, missing required fields, illegal types, and extra/unknown fields.
- Rejects duplicate keys in mappings.
- Rejects binding syntax that violates tagged variant discriminator rules.
- **Does NOT check** whether a referenced task exists.
- **Does NOT check** for graph cycles or dependency self-references.
- **Does NOT enforce** non-empty task collections; `tasks: {}` parses into a structurally valid `CandidateWorkflowSpec`.
- Output: An immutable `CandidateWorkflowSpec` instance, which may represent an invalid DAG.

### 5.2 Semantic Phase Invariants (ADR-004)
- Operates on a structurally well-formed `CandidateWorkflowSpec`.
- Validates graph topology: presence of referenced dependencies, absence of self-dependencies, absence of duplicate dependencies, and absence of cycles.
- Validates data flow bindings: verifies that `TaskOutput` references point to a declared direct upstream dependency of the consuming task.
- Validates workflow output bindings: verifies that all output sources point to existing tasks defined in the specification.
- Evaluates domain constraints: rejects empty task sets (`EMPTY_WORKFLOW`).
- Output: An immutable `ValidatedWorkflowSpec` coupled with its derived `CanonicalGraph`.

---

## 6. Definition Ingestion Data Model

### 6.1 External Boundary DTOs (Pydantic v2 with `extra='forbid'`)
External boundary models parse incoming dictionaries from the YAML loader. All models enforce strict typing, forbid unexpected fields, and require explicit configuration without unapproved defaults:

```python
from typing import Annotated, Literal, Union
from pydantic import BaseModel, ConfigDict, Field


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
    )


# --- Input Binding External DTOs ---


class LiteralBindingDTO(StrictBaseModel):
    type: Literal["literal"]
    value: object  # Must validate against JSON value semantics


class WorkflowInputBindingDTO(StrictBaseModel):
    type: Literal["workflow_input"]


class TaskOutputBindingDTO(StrictBaseModel):
    type: Literal["task_output"]
    task: str


InputBindingDTO = Annotated[
    Union[LiteralBindingDTO, WorkflowInputBindingDTO, TaskOutputBindingDTO],
    Field(discriminator="type"),
]

# --- Workflow Output Binding External DTOs (Frozen V1 Baseline) ---


class WorkflowTaskOutputBindingDTO(StrictBaseModel):
    type: Literal["task_output"]
    task: str


# Only the frozen V1 workflow output source family is permitted
WorkflowOutputBindingDTO = WorkflowTaskOutputBindingDTO

# --- Task & Workflow External DTOs ---


class TaskDefinitionDTO(StrictBaseModel):
    activity_type: str
    dependencies: list[str] = Field(default_factory=list)
    input_bindings: dict[str, InputBindingDTO] = Field(default_factory=dict)
    max_attempts: int  # Explicitly required: no unapproved default


class WorkflowDefinitionDTO(StrictBaseModel):
    workflow_name: str
    tasks: dict[str, TaskDefinitionDTO]  # No min_length: empty tasks allowed structurally
    output_bindings: dict[str, WorkflowOutputBindingDTO] = Field(default_factory=dict)
```

### 6.2 External YAML Definition Structure
The V1 external definition format mandates a mapping-keyed task structure. Keys in the `tasks` mapping define the unique `TaskDefinitionId`. Inner redundant IDs are prohibited to prevent inconsistency:

```yaml
workflow_name: order_fulfillment

tasks:
  verify_inventory:
    activity_type: inventory.verify
    dependencies: []
    input_bindings:
      order_id:
        type: workflow_input
    max_attempts: 3

  charge_customer:
    activity_type: payment.charge
    dependencies:
      - verify_inventory
    input_bindings:
      amount:
        type: literal
        value: 15000
      order_id:
        type: workflow_input
    max_attempts: 2

  dispatch_goods:
    activity_type: shipping.dispatch
    dependencies:
      - charge_customer
    input_bindings:
      payment_receipt:
        type: task_output
        task: charge_customer
    max_attempts: 1

output_bindings:
  shipment_confirmation:
    type: task_output
    task: dispatch_goods
```

### 6.3 Frozen Domain Specification Types
Domain types are pure, frozen dataclasses defined in LLD-01:

```python
from dataclasses import dataclass
from typing import Mapping, Sequence

from nexusflow.domain.definitions.bindings import (
    InputBinding,
    WorkflowTaskOutputBinding,
)
from nexusflow.domain.definitions.task_definition import TaskDefinition
from nexusflow.domain.values import TaskDefinitionId


@dataclass(frozen=True, slots=True)
class CandidateWorkflowSpec:
    """
    Unvalidated candidate specification resulting from structural normalization.
    Preserves raw user dependency declarations (including duplicates, self-references,
    empty tasks, and invalid IDs) so the semantic validator can produce accurate diagnostics.
    """

    workflow_name: str
    tasks: Sequence[TaskDefinition]
    raw_dependencies: Mapping[TaskDefinitionId, tuple[str, ...]]
    output_bindings: Mapping[str, WorkflowTaskOutputBinding]


@dataclass(frozen=True, slots=True)
class ValidatedWorkflowSpec:
    """
    Immutable, semantically validated specification.
    Guaranteed by ADR-004 to represent an acyclic DAG with all dependencies,
    activity types, and whole-value bindings verified.
    """

    workflow_name: str
    tasks: Mapping[TaskDefinitionId, TaskDefinition]
    output_bindings: Mapping[str, WorkflowTaskOutputBinding]

    def get_task(self, task_id: TaskDefinitionId) -> TaskDefinition:
        if task_id not in self.tasks:
            raise KeyError(f"TaskDefinitionId '{task_id}' does not exist in specification.")
        return self.tasks[task_id]
```

---

## 7. Task & Data Flow Binding Semantics

### 7.1 Domain Identity Semantics vs. Operational Ingress Limits
1. **Domain Identity Semantics:**
   - Exact string equality.
   - Case-sensitive (`"order_step"` $\neq$ `"Order_Step"`).
   - No trimming, folding, or whitespace normalization.
   - Non-empty string requirement enforced at domain type instantiation.
2. **Operational Ingress Safety Limits:**
   - Ingress limits (e.g., maximum string lengths) are operational configuration parameters owned by ADR-023 / LLD-09.
   - They do not define the semantic meaning of `TaskDefinitionId` or `ActivityType`.

### 7.2 Input Binding Variants
Conforming to ADR-010 Section 10.3, input bindings support only whole-value injection:

1. **`LiteralBinding(value: JsonValue)`:** Injects an immutable JSON scalar, list, or mapping. The value is deeply validated via `freeze_json`. Null values (`null`) are permitted as valid JSON null.
2. **`WorkflowInputBinding()`:** Injects the entire immutable input payload provided at workflow execution start. Prohibits nested paths, expressions, or key selectors.
3. **`TaskOutputBinding(upstream_task_id: TaskDefinitionId)`:** Injects the entire committed output of a declared, direct upstream task dependency.

### 7.3 Workflow Output Binding Model
Conforming to the approved ADR-010 / LLD-01 baseline, named workflow outputs in V1 bind exclusively to the authoritative output of a task:

$$\text{output\_name} \longrightarrow \text{WorkflowTaskOutputBinding(source\_task\_id)}$$

- **`WorkflowTaskOutputBinding(source_task_id: TaskDefinitionId)`:** References the whole committed output of any task defined in the workflow. The source task is not required to be a graph leaf.
- Successful workflow execution resolution produces a named mapping:
  ```json
  {
    "output_name": <whole task output>
  }
  ```
- **Absence of Output Bindings:** If `output_bindings` is empty, successful workflow execution commits as JSON `null` (`has_output=True, workflow_output='null'::jsonb`).

---

## 8. Canonical Graph Representation & Cycle Detection

### 8.1 Graph Invariants & Derivation Relationship
1. **Derivation Relationship:** Graph semantics are derived solely from IWS task and dependency declarations.
2. **Validation Construction:** During ADR-004 semantic validation, a candidate graph is constructed from reference-valid task definitions to verify acyclicity.
3. **Post-Promotion Authority:** After successful validation, the `CanonicalGraph` is considered derived from the resulting `ValidatedWorkflowSpec`. The graph is never an independent semantic source of truth.
4. **Graph Properties:**
   - Nodes, incoming edges (`dependencies`), and outgoing edges (`dependents`) are stored in immutable sets and mappings.
   - Edge Direction: If task $B$ declares dependency on task $A$, the directed edge is $A \to B$ ($A$ precedes $B$).
   - Topological Flexibility: Disconnected DAGs, multiple roots, and multiple leaves are valid and supported.

### 8.2 Canonical Graph Data Structure
```python
@dataclass(frozen=True, slots=True)
class CanonicalGraph:
    """
    Sparse bidirectional adjacency DAG derived from task and dependency declarations.
    All collections are deeply immutable.
    """

    nodes: frozenset[TaskDefinitionId]
    dependencies: Mapping[TaskDefinitionId, frozenset[TaskDefinitionId]]  # Incoming: B -> {A}
    dependents: Mapping[TaskDefinitionId, frozenset[TaskDefinitionId]]  # Outgoing: A -> {B}

    def get_upstream_dependencies(self, task_id: TaskDefinitionId) -> frozenset[TaskDefinitionId]:
        return self.dependencies.get(task_id, frozenset())

    def get_downstream_dependents(self, task_id: TaskDefinitionId) -> frozenset[TaskDefinitionId]:
        return self.dependents.get(task_id, frozenset())

    def is_root(self, task_id: TaskDefinitionId) -> bool:
        return len(self.dependencies.get(task_id, frozenset())) == 0

    def is_leaf(self, task_id: TaskDefinitionId) -> bool:
        return len(self.dependents.get(task_id, frozenset())) == 0
```

### 8.3 Separation of $O(V+E)$ Cycle Detection vs. Deterministic Topological Sorting
NexusFlow strictly separates:
1. **Core Canonical Graph Construction & Cycle Detection:** Must execute strictly in linear time $O(V+E)$ without sorting.
2. **Optional Deterministic Topological Ordering:** If needed for deterministic diagnostics or evaluation, it can be derived separately. Sorting costs ($O((V+E) \log V)$) are never conflated with DAG validation invariants.
3. **Non-Authoritative Status:** Multiple valid topological orders exist for any non-trivial DAG. Scheduler correctness in LLD-04 relies exclusively on adjacency and in-degree transition readiness, never on a single authoritative total order.

```python
from collections import deque


def build_canonical_graph_and_verify_acyclic(
    tasks: Mapping[TaskDefinitionId, TaskDefinition],
) -> tuple[CanonicalGraph, list[str]]:
    """
    Constructs the CanonicalGraph and verifies acyclicity using Kahn's algorithm strictly in O(V+E).
    Precondition: All dependencies referenced in task.dependencies MUST exist in tasks.
    Performs NO node sorting to guarantee pure O(V+E) linear complexity.
    Returns (graph, cycle_errors).
    """
    nodes = frozenset(tasks.keys())
    in_degree: dict[TaskDefinitionId, int] = {t: 0 for t in nodes}
    dependencies: dict[TaskDefinitionId, set[TaskDefinitionId]] = {t: set() for t in nodes}
    dependents: dict[TaskDefinitionId, set[TaskDefinitionId]] = {t: set() for t in nodes}

    # Step 1: Build bidirectional adjacency and compute in-degrees in O(V + E)
    for task_id, task in tasks.items():
        for dep_id in task.dependencies:
            if dep_id not in nodes:
                raise ValueError(
                    f"Precondition violated: dependency '{dep_id}' not found in tasks."
                )
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
        return canonical_graph, [
            f"Graph contains a cycle involving tasks: {', '.join(cycle_nodes)}"
        ]

    return canonical_graph, []


def compute_deterministic_topological_order(
    graph: CanonicalGraph,
) -> tuple[TaskDefinitionId, ...]:
    """
    Derived convenience helper: Computes a deterministic topological order in O((V + E) log V)
    by sorting node keys. This order is non-authoritative and recomputable.
    """
    in_degree: dict[TaskDefinitionId, int] = {
        t: len(graph.get_upstream_dependencies(t)) for t in graph.nodes
    }

    # Priority queue / sorted deque for deterministic evaluation
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
```

---

## 9. Semantic Validation Rules & Deterministic Error Model

### 9.1 Typed Validation Error Model
```python
from enum import StrEnum
from typing import Mapping, TypeAlias

JsonObject: TypeAlias = Mapping[str, JsonValue]


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
```

### 9.2 Ordered Semantic Validation Pipeline
```
[ Phase 1: Minimum Task Collection Check ]
  │ Verify len(tasks) >= 1 (Emit EMPTY_WORKFLOW)
  ▼
[ Phase 2: Dependency Integrity Checks ]
  │ For each task:
  │   - Check max_attempts against operational admission limit
  │   - Reject duplicate dependency declarations (DUPLICATE_DEPENDENCY)
  │   - Reject self-dependencies (SELF_DEPENDENCY)
  │   - Verify dependency exists in task set (UNKNOWN_DEPENDENCY)
  ▼
[ Phase 3: Task Input Binding Checks ]
  │ For each task input binding:
  │   - If TaskOutput: verify source task exists (UNKNOWN_TASK_OUTPUT_SOURCE)
  │   - If TaskOutput: verify source is a DIRECT dependency (TASK_OUTPUT_SOURCE_NOT_DEPENDENCY)
  ▼
[ Phase 4: Workflow Output Binding Checks ]
  │ For each workflow output binding:
  │   - Verify source task exists in workflow (UNKNOWN_WORKFLOW_OUTPUT_SOURCE)
  ▼
[ Phase 5: Canonical Graph Construction & Cycle Detection in O(V+E) ]
  │ If phases 1-4 pass without reference errors:
  │   - Construct CanonicalGraph strictly in O(V+E)
  │   - Execute linear Kahn reduction (CYCLE_DETECTED)
  ▼
[ Phase 6: Validated Promotion ]
  │ If all phases pass:
  │   - Return ValidatedDefinitionResult(spec, graph)
```

### 9.3 Semantic Validator Implementation
```python
@dataclass(frozen=True, slots=True)
class ValidatedDefinitionResult:
    spec: ValidatedWorkflowSpec
    graph: CanonicalGraph


@dataclass(frozen=True, slots=True)
class ValidationOutcome:
    success: ValidatedDefinitionResult | None
    errors: tuple[DefinitionValidationError, ...]


class SemanticValidator:
    """
    Pure in-memory semantic validator implementing ADR-004.
    Accumulates errors across non-dependent checks up to max_errors.
    """

    def __init__(
        self,
        max_attempts_admission_limit: int = 100,  # Provisional V1 operational limit (LLD-09)
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
```

---

## 10. Request Fingerprinting & Idempotency Integration

LLD-02 requires a deterministic `RequestFingerprint` for the `commit_registered_definition` operation.

### 10.1 Fingerprint Computation Rules
1. **Canonical Normalized Input:** The fingerprint is computed from the structurally validated and normalized dictionary representation of the registration request, *not* the raw YAML text.
2. **Formatting Equivalence:** Harmless formatting variations (indentation, blank lines, YAML comments, mapping key order in the external file) produce identical fingerprints.
3. **No Non-Semantic Metadata:** Ephemeral values (generated `DefinitionId`, submission timestamps, client credentials) are excluded.
4. **Deterministic JSON Serialization:** Encoded via standard JSON with alphabetically sorted keys, fixed non-spaced separators `(",", ":")`, and UTF-8 encoding.

### 10.2 Cryptographic Fingerprint Helper
```python
import hashlib
import json
from typing import Any

from nexusflow.domain.values import RequestFingerprint


def compute_registration_fingerprint(normalized_dto_dict: dict[str, Any]) -> RequestFingerprint:
    """
    Computes a deterministic SHA-256 RequestFingerprint from normalized definition DTOs.
    Sorts mapping keys and uses compact separators to guarantee semantic invariance.
    """
    canonical_bytes = json.dumps(
        normalized_dto_dict,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")

    digest_hex = hashlib.sha256(canonical_bytes).hexdigest()
    return RequestFingerprint(digest=digest_hex)
```

---

## 11. Persistence Serialization & Deserialization Codec

The persistence codec operates strictly against LLD-02 Section 6.1 (`registered_definitions.validated_iws`).

### 11.1 Persistence Serialization Codec Contract
- **Output Types:** Returns exclusively standard JSON-native Python structures: `dict` with string keys, `list`, `str`, `int`, finite `float`, `bool`, and `None`.
- **Prohibited Returns:** Never returns `MappingProxyType`, `tuple`, domain IDs (`TaskDefinitionId`), enums (`ActivityType`), or dataclasses.
- **Fail-Closed Thawing:** Every internal domain `JsonValue` passes through `thaw_json(...)`. If an invalid value (non-string object key, NaN, Infinity, unsupported type) reaches the serializer, it fails immediately by raising `TypeError` or `ValueError`. It never stringifies non-string keys.

```python
from typing import Any
from nexusflow.domain.definitions.bindings import (
    LiteralBinding,
    WorkflowInputBinding,
    TaskOutputBinding,
    WorkflowTaskOutputBinding,
)
from nexusflow.domain.definitions.validated import ValidatedWorkflowSpec


def serialize_validated_spec(spec: ValidatedWorkflowSpec) -> dict[str, Any]:
    """
    Serializes a ValidatedWorkflowSpec into the exact LLD-02 JSONB persistence schema.
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
                # Every JsonValue must be thawed from domain MappingProxyType/tuple into dict/list
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
```

### 11.2 Trusted Recovery Deserializer with Integrity Verification
Engine recovery loads `validated_iws` directly from PostgreSQL. To protect the runtime from corrupted persistence data, the deserializer performs strict structural validation and type checking with **zero type coercion**:

```python
class PersistenceIntegrityError(Exception):
    """Raised when persisted specification in the database violates integrity invariants."""

    pass


def deserialize_validated_spec(data: Any) -> ValidatedWorkflowSpec:
    """
    Reconstructs an immutable ValidatedWorkflowSpec directly from persisted JSONB.
    Performs fail-closed integrity validation on the durable structure.
    Strictly verifies physical JSON types with zero coercion (no int(...) or bool conversion).
    """
    try:
        # Rule 1: Root container shape verification
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

            # Strict dependencies container verification (no string masquerading as list)
            raw_deps = t_data.get("dependencies", [])
            if not isinstance(raw_deps, list):
                raise PersistenceIntegrityError(
                    f"Task '{t_id_str}' dependencies must be a list, got {type(raw_deps).__name__}."
                )

            deps_set = set()
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

            # Strict max_attempts decoding: must be exact int, NOT bool, >= 1
            raw_max_attempts = t_data.get("max_attempts")
            if (
                type(raw_max_attempts) is not int
            ):  # type(...) is not int rejects bool (bool is subclass of int)
                raise PersistenceIntegrityError(
                    f"Task '{t_id_str}' max_attempts must be exact integer, got {type(raw_max_attempts).__name__} ({raw_max_attempts!r})."
                )
            if raw_max_attempts < 1:
                raise PersistenceIntegrityError(
                    f"Task '{t_id_str}' has invalid stored max_attempts {raw_max_attempts} (< 1)."
                )
            max_attempts = raw_max_attempts

            # Strict input bindings container verification
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
                    # Re-freeze literal value into domain immutable representation
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

        # Strict output bindings container verification
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

        # Verify graph acyclicity on stored tasks strictly in O(V+E)
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
```

---

## 12. Ingestion Application Service Flow

Application use case boundaries use typed domain outcomes, leaving HTTP protocol and status code mappings entirely to LLD-08:

```python
from enum import StrEnum
from uuid import uuid4
from datetime import datetime, timezone
from dataclasses import dataclass

from nexusflow.domain.values import (
    DefinitionId,
    IdempotencyKey,
    RequestFingerprint,
)
from nexusflow.domain.ports.definition_persistence import DefinitionPersistencePort, CommitStatus


class RegistrationOutcomeStatus(StrEnum):
    CREATED = "CREATED"
    IDEMPOTENT_MATCH = "IDEMPOTENT_MATCH"
    STRUCTURAL_VALIDATION_FAILURE = "STRUCTURAL_VALIDATION_FAILURE"
    SEMANTIC_VALIDATION_FAILURE = "SEMANTIC_VALIDATION_FAILURE"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    PERSISTENCE_FAILURE = "PERSISTENCE_FAILURE"


@dataclass(frozen=True, slots=True)
class RegisterDefinitionCommand:
    raw_yaml: str
    idempotency_key: IdempotencyKey | None


@dataclass(frozen=True, slots=True)
class RegisterDefinitionResult:
    status: RegistrationOutcomeStatus
    definition_id: DefinitionId | None = None
    errors: tuple[DefinitionValidationError, ...] = ()


class RegisterDefinitionUseCase:
    """
    Coordinates safe parsing, structural normalization, semantic validation,
    request fingerprinting, and LLD-02 persistence invocation.
    """

    def __init__(
        self,
        parser: DefinitionParser,
        validator: SemanticValidator,
        persistence_port: DefinitionPersistencePort,
        max_payload_bytes: int = 1_048_576,  # Provisional V1 operational limit (LLD-09)
    ) -> None:
        self._parser = parser
        self._validator = validator
        self._persistence = persistence_port
        self._max_payload_bytes = max_payload_bytes

    async def execute(self, command: RegisterDefinitionCommand) -> RegisterDefinitionResult:
        # Step 1: Enforce payload byte limit before parsing
        encoded_bytes = command.raw_yaml.encode("utf-8")
        if len(encoded_bytes) > self._max_payload_bytes:
            return RegisterDefinitionResult(
                status=RegistrationOutcomeStatus.STRUCTURAL_VALIDATION_FAILURE,
                errors=(
                    DefinitionValidationError(
                        code=DefinitionValidationCode.PAYLOAD_TOO_LARGE,
                        message=f"Payload size ({len(encoded_bytes)} bytes) exceeds operational limit ({self._max_payload_bytes} bytes).",
                        path="",
                    ),
                ),
            )

        # Step 2: Safe YAML parsing, complexity checks, and structural DTO validation
        parse_res = self._parser.parse(command.raw_yaml)
        if not parse_res.success:
            return RegisterDefinitionResult(
                status=RegistrationOutcomeStatus.STRUCTURAL_VALIDATION_FAILURE,
                errors=parse_res.errors,
            )

        candidate = parse_res.candidate
        assert candidate is not None

        # Step 3: Semantic validation & graph construction
        val_res = self._validator.validate(candidate)
        if not val_res.success:
            return RegisterDefinitionResult(
                status=RegistrationOutcomeStatus.SEMANTIC_VALIDATION_FAILURE,
                errors=val_res.errors,
            )

        validated_spec = val_res.success.spec

        # Step 4: Compute deterministic request fingerprint
        fingerprint = compute_registration_fingerprint(parse_res.normalized_dict)

        # Step 5: Allocate DefinitionId UUIDv4
        new_def_id = DefinitionId(value=uuid4())
        now_utc = datetime.now(timezone.utc)

        # Step 6: Commit to persistence (LLD-02 consistency group)
        outcome, committed_def_id = await self._persistence.register_definition_if_absent(
            definition_id=new_def_id,
            workflow_name=validated_spec.workflow_name,
            spec=validated_spec,
            raw_yaml=command.raw_yaml,
            idempotency_key=command.idempotency_key,
            fingerprint=fingerprint,
            now_utc=now_utc,
        )

        if outcome.status == CommitStatus.COMMITTED:
            status = (
                RegistrationOutcomeStatus.CREATED
                if committed_def_id == new_def_id
                else RegistrationOutcomeStatus.IDEMPOTENT_MATCH
            )
            return RegisterDefinitionResult(status=status, definition_id=committed_def_id)
        elif outcome.status == CommitStatus.PRECONDITION_FAILED:
            return RegisterDefinitionResult(
                status=RegistrationOutcomeStatus.IDEMPOTENCY_CONFLICT,
                errors=(),
            )
        else:
            return RegisterDefinitionResult(
                status=RegistrationOutcomeStatus.PERSISTENCE_FAILURE,
                errors=(),
            )
```

---

## 13. Operational Security & Complexity Limits

Operational limits parameterize operational safety policies (ADR-023) and are distinct from semantic identity rules:

| Configuration Key | Provisional V1 Default | Scope | Enforcement Mechanism |
| :--- | :--- | :--- | :--- |
| `nexusflow.ingestion.max_payload_bytes` | `1048576` (1 MiB) | Ingestion | Pre-parse byte length check on input stream |
| `nexusflow.ingestion.max_yaml_depth` | `20` | Ingestion | Post-compose AST traversal depth check |
| `nexusflow.ingestion.max_yaml_nodes` | `2000` | Ingestion | Post-compose AST total node counter |
| `nexusflow.ingestion.allow_yaml_aliases` | `False` | Ingestion | Post-compose AST rejection of anchors/aliases |
| `nexusflow.ingestion.max_attempts_admission_limit`| `100` | Ingestion | Semantic validator check against task `max_attempts` |
| `nexusflow.ingestion.max_validation_errors` | `50` | Ingestion | Accumulator cap in semantic validator |

*Note: The values above are provisional V1 operational defaults, finalized in LLD-09.*

---

## 14. Testing Strategy

### 14.1 Parser & Structural Unit Tests (`tests/unit/test_yaml_parser.py`)
- **Empty Task Workflow:** Verify that `tasks: {}` parses successfully into a `CandidateWorkflowSpec` with zero tasks.
- **Syntax Errors:** Verify that malformed YAML returns `INVALID_YAML`.
- **Duplicate Keys:** Verify that duplicate keys raise `DUPLICATE_KEY`.
- **Unknown Fields:** Verify that unknown attributes (e.g., `max_attempt: 3`) raise `UNKNOWN_FIELD`.
- **Explicit max_attempts:** Verify that omitting `max_attempts` on a task definition raises `MISSING_FIELD`.
- **Type Surprises:** Verify that YAML dates, timestamps, NaN, Infinity, and non-string mapping keys raise `INVALID_FIELD_TYPE`.
- **Multi-Document Streams:** Verify that streams with `---` multi-documents raise `INVALID_YAML`.
- **Empty / Scalar Roots:** Verify that empty documents or scalar roots raise `INVALID_ROOT`.

### 14.2 Binding Unit Tests (`tests/unit/test_bindings.py`)
- Test valid input variants: `literal`, `workflow_input`, `task_output`.
- Test valid workflow output variant: `task_output`.
- Test that unapproved workflow output bindings (`type: literal`, `type: workflow_input`) fail structurally.
- Test rejection of unknown binding discriminators.
- Test rejection of variant field pollution (e.g., `workflow_input` supplying a `value` or `path`).
- Test that literal `null` is correctly parsed as JSON `null` (`None`).

### 14.3 YAML Complexity & Anchor/Alias Defense Tests (`tests/unit/test_yaml_complexity.py`)
- **Oversized Stream:** Verify that payloads exceeding `max_payload_bytes` are rejected before parser execution.
- **Deep Nesting:** Verify that deeply nested YAML structures exceed `max_yaml_depth` and raise `YAML_COMPLEXITY_EXCEEDED`.
- **Node Count Limit:** Verify that trees with excessive collection elements exceed `max_yaml_nodes` and raise `YAML_COMPLEXITY_EXCEEDED`.
- **Concrete Alias Rejection:** Verify that input containing YAML anchors and aliases:
  ```yaml
  a: &base
    x: 1
  b: *base
  ```
  is detected and rejected during post-compose Node inspection with `YAML_COMPLEXITY_EXCEEDED`.

### 14.4 Semantic Validation Unit Tests (`tests/unit/test_semantic_validator.py`)
- **Empty Workflow:** Verify that a `CandidateWorkflowSpec` with 0 tasks raises `EMPTY_WORKFLOW`.
- **Single-Task Workflow:** Verify that a single task without dependencies promotes successfully.
- **Dependency Invariants:**
  - Verify missing dependency raises `UNKNOWN_DEPENDENCY`.
  - Verify self-dependency raises `SELF_DEPENDENCY`.
  - Verify duplicate dependency declaration raises `DUPLICATE_DEPENDENCY`.
- **TaskOutput Invariants:**
  - Verify `TaskOutput` referencing an unknown task raises `UNKNOWN_TASK_OUTPUT_SOURCE`.
  - Verify `TaskOutput` referencing a non-dependency task raises `TASK_OUTPUT_SOURCE_NOT_DEPENDENCY`.
  - Verify transitive `TaskOutput` referencing an indirect dependency is rejected.
- **Workflow Output Invariants:**
  - Verify workflow output referencing an unknown task raises `UNKNOWN_WORKFLOW_OUTPUT_SOURCE`.
  - Verify workflow output referencing an intermediate (non-leaf) task is accepted.
- **Topology Acceptance:**
  - Multiple roots: Accepted.
  - Multiple leaves: Accepted.
  - Disconnected DAG (multiple isolated components): Accepted.

### 14.5 Graph & Cycle Detection Tests (`tests/unit/test_canonical_graph.py`)
- **Precondition Contract:** Verify that calling `build_canonical_graph_and_verify_acyclic` with an unknown dependency raises `ValueError`.
- **Pure Linear Complexity:** Verify that cycle detection and graph construction execute strictly in $O(V+E)$ without invoking sort routines.
- **Adjacency Directions:** Verify that dependency $A \to B$ produces $A \in \text{dependencies}(B)$ and $B \in \text{dependents}(A)$.
- **Simple Cycle:** Verify that $A \to B \to A$ is rejected with `CYCLE_DETECTED`.
- **Complex Multi-Node Cycle:** Verify that $A \to B \to C \to D \to B$ is rejected and identifies participating nodes.
- **Optional Topological Sorting:** Verify that `compute_deterministic_topological_order` produces a valid topological sequence in $O((V+E) \log V)$ and is isolated from graph creation.

### 14.6 Property-Based Testing with Hypothesis (`tests/property/test_dag_properties.py`)
- **Acyclic DAG Generation:** Generate random valid DAGs; verify that canonical graph operations execute correctly in $O(V+E)$.
- **Cycle Injection:** Inject a single back-edge into a randomly generated DAG; assert that semantic validation unconditionally fails with `CYCLE_DETECTED`.

### 14.7 Determinism & Fingerprint Tests (`tests/unit/test_fingerprint.py`)
- Assert that variations in YAML indentation, comment placement, and task dictionary key ordering yield identical SHA-256 request fingerprints.
- Assert that `flag: true` and `flag: "true"` yield different request fingerprints.
- Assert that semantic errors are reported in deterministic order across repeated runs.

### 14.8 Persistence Codec & Integrity Tests (`tests/unit/test_persistence_codec.py`)
- **Round-Trip Fidelity with Rich Literals:** Verify that specifications containing nested object literals (`MappingProxyType`), nested array literals (`tuple`), booleans, finite floats, integers, and JSON `null` serialize via `thaw_json` into ordinary Python `dict`/`list` structures and deserialize back into exact domain models.
- **Strict Shape Integrity Failures (No Coercion):**
  - `"max_attempts": "3"` (string) raises `PersistenceIntegrityError`.
  - `"max_attempts": 1.5` (float) raises `PersistenceIntegrityError`.
  - `"max_attempts": true` (bool) raises `PersistenceIntegrityError`.
  - `"dependencies": "task_a"` (string masquerading as list) raises `PersistenceIntegrityError`.
  - `"input_bindings": []` (list masquerading as mapping) raises `PersistenceIntegrityError`.
  - `"output_bindings": []` (list masquerading as mapping) raises `PersistenceIntegrityError`.
  - Non-string dependency item in list raises `PersistenceIntegrityError`.
  - Malformed binding payload (not a mapping) raises `PersistenceIntegrityError`.
- **Relational Integrity Violations:**
  - Missing referenced dependency in stored JSONB raises `PersistenceIntegrityError`.
  - Stored `TaskOutput` not declared as dependency raises `PersistenceIntegrityError`.
  - Stored spec containing graph cycle raises `PersistenceIntegrityError`.
  - Unknown workflow output target raises `PersistenceIntegrityError`.

---

## 15. Inventories & Reference Matrices

### 15.1 Definition Validation Error Matrix
| Phase | Condition | Error Code | Example Path | Aggregation Policy |
| :--- | :--- | :--- | :--- | :--- |
| Structural | Payload exceeds configured byte limit | `PAYLOAD_TOO_LARGE` | `""` | Fatal (Immediate Return) |
| Structural | AST depth / node count / alias violated | `YAML_COMPLEXITY_EXCEEDED` | `""` | Fatal (Immediate Return) |
| Structural | Malformed YAML syntax | `INVALID_YAML` | `""` | Fatal (Immediate Return) |
| Structural | Multiple YAML documents in stream | `INVALID_YAML` | `""` | Fatal (Immediate Return) |
| Structural | Duplicate mapping key in YAML | `DUPLICATE_KEY` | `tasks.charge` | Fatal (Immediate Return) |
| Structural | Root element is not a mapping | `INVALID_ROOT` | `""` | Fatal (Immediate Return) |
| Structural | Missing required field | `MISSING_FIELD` | `tasks.ship.activity_type` | Accumulable across DTOs |
| Structural | Unexpected / extra field present | `UNKNOWN_FIELD` | `tasks.ship.max_attempt` | Accumulable across DTOs |
| Structural | Scalar type violates JSON model | `INVALID_FIELD_TYPE` | `tasks.step.input_bindings.date` | Accumulable across DTOs |
| Structural | Unknown input binding discriminator | `INVALID_BINDING` | `tasks.step.input_bindings.x.type`| Accumulable across DTOs |
| Semantic | Workflow contains 0 task definitions | `EMPTY_WORKFLOW` | `tasks` | Fatal (Immediate Return) |
| Semantic | Task declares unknown dependency ID | `UNKNOWN_DEPENDENCY` | `tasks.b.dependencies[0]` | Accumulable |
| Semantic | Task declares self as dependency | `SELF_DEPENDENCY` | `tasks.a.dependencies[0]` | Accumulable |
| Semantic | Duplicate dependency in list | `DUPLICATE_DEPENDENCY` | `tasks.a.dependencies[1]` | Accumulable |
| Semantic | TaskOutput references unknown task | `UNKNOWN_TASK_OUTPUT_SOURCE` | `tasks.b.input_bindings.in.task` | Accumulable |
| Semantic | TaskOutput source is not a dependency | `TASK_OUTPUT_SOURCE_NOT_DEPENDENCY` | `tasks.c.input_bindings.in.task` | Accumulable |
| Semantic | Workflow output references unknown task | `UNKNOWN_WORKFLOW_OUTPUT_SOURCE` | `output_bindings.result.task` | Accumulable |
| Semantic | Graph contains directed cycle | `CYCLE_DETECTED` | `tasks` | Terminal Semantic Error |
| Semantic | max_attempts exceeds limit | `ATTEMPTS_EXCEEDED_LIMIT` | `tasks.a.max_attempts` | Accumulable |

### 15.2 Data Flow Binding Matrix
| External Binding Type | Required Fields | Forbidden Fields | Domain Target Class | Semantic Preconditions |
| :--- | :--- | :--- | :--- | :--- |
| `literal` (Input) | `type: literal`, `value` | `task`, `path` | `LiteralBinding` | Value must satisfy RFC 8259 JSON, `freeze_json`, and thaw via `thaw_json`. |
| `workflow_input` (Input) | `type: workflow_input` | `value`, `task`, `path` | `WorkflowInputBinding` | None (Whole-value injection). |
| `task_output` (Input) | `type: task_output`, `task` | `value`, `path` | `TaskOutputBinding` | Source task must exist AND be a direct declared dependency. |
| `task_output` (Output) | `type: task_output`, `task` | `value`, `path` | `WorkflowTaskOutputBinding`| Source task must exist in workflow (need not be a leaf). |

### 15.3 Semantic Validation Matrix
| Validation Check | Phase | Algorithm | Complexity | Error Code |
| :--- | :--- | :--- | :--- | :--- |
| Workflow Non-Empty | Semantic (P1) | Length inspection | $O(1)$ | `EMPTY_WORKFLOW` |
| Dependency Existence | Semantic (P2) | Set membership lookup | $O(V + E)$ | `UNKNOWN_DEPENDENCY` |
| Self-Dependency | Semantic (P2) | Equality comparison | $O(V + E)$ | `SELF_DEPENDENCY` |
| Duplicate Dependency | Semantic (P2) | Seen set insertion | $O(V + E)$ | `DUPLICATE_DEPENDENCY` |
| TaskOutput Direct Dependency | Semantic (P3) | Adjacency set membership | $O(B)$ | `TASK_OUTPUT_SOURCE_NOT_DEPENDENCY` |
| Workflow Output Existence | Semantic (P4) | Set membership lookup | $O(O)$ | `UNKNOWN_WORKFLOW_OUTPUT_SOURCE` |
| Acyclicity & Topology | Semantic (P5) | Pure Linear Kahn's Algorithm | $O(V + E)$ | `CYCLE_DETECTED` |

### 15.4 Canonical Graph Operations Contract
| Operation | Input Signature | Output Signature | Time Complexity | Failure Modes |
| :--- | :--- | :--- | :--- | :--- |
| `build_canonical_graph_and_verify_acyclic` | `Mapping[TaskDefinitionId, TaskDefinition]` | `CanonicalGraph, list[str]` | $O(V + E)$ | Precondition violation (unknown dependency raises `ValueError`) or cycle detected. |
| `compute_deterministic_topological_order` | `CanonicalGraph` | `tuple[TaskDefinitionId, ...]` | $O((V + E) \log V)$ | None (precondition: graph is verified acyclic). Non-authoritative convenience. |
| `get_upstream_dependencies` | `TaskDefinitionId` | `frozenset[TaskDefinitionId]` | $O(1)$ | None (returns empty frozenset if task isolated). |
| `get_downstream_dependents` | `TaskDefinitionId` | `frozenset[TaskDefinitionId]` | $O(1)$ | None (returns empty frozenset if task isolated). |
| `is_root` | `TaskDefinitionId` | `bool` | $O(1)$ | None. |
| `is_leaf` | `TaskDefinitionId` | `bool` | $O(1)$ | None. |

---

## 16. End-to-End Pipeline Sequence

```
RegisterDefinitionUseCase.execute(command)
    │
    │ 1. Check byte size <= max_payload_bytes (Pre-parse check)
    ├─────────────────────────────────────────┐
    │ (Size exceeded)                         │ (Size valid)
    ▼                                         ▼
Return STRUCTURAL_VALIDATION_FAILURE      DefinitionParser (ruamel.yaml)
                                              │
                                              │ 2. typ='safe', pure=True, allow_duplicate_keys=False
                                              │ 3. AST complexity & depth check (post-compose Node traversal)
                                              │ 4. Single-document, mapping root
                                              │ 5. Parse to Boundary DTOs (extra='forbid')
                                              │    (Allows tasks: {} without structural failure)
                                              ├─────────────────────────────────────────┐
                                              │ (Structural error)                      │ (Valid syntax)
                                              ▼                                         ▼
                                          Return STRUCTURAL_VALIDATION_FAILURE      Construct CandidateWorkflowSpec
                                                                                    (Freeze JSON, Domain IDs)
                                                                                        │
                                                                                        ▼
                                                                                    SemanticValidator
                                                                                        │
                                                                                        │ 6. Check non-empty tasks (EMPTY_WORKFLOW)
                                                                                        │ 7. Check dependencies & bindings
                                                                                        │ 8. Build CanonicalGraph (Preconditions verified)
                                                                                        │ 9. Execute linear Kahn's algorithm O(V+E)
                                                                                        ├─────────────────────────────────────────┐
                                                                                        │ (Semantic error)                        │ (Valid DAG)
                                                                                        ▼                                         ▼
                                                                                    Return SEMANTIC_VALIDATION_FAILURE        Promote to ValidatedWorkflowSpec
                                                                                                                                  │
                                                                                        ┌─────────────────────────────────────────┘
                                                                                        ▼
                                                                                    Compute RequestFingerprint (SHA-256)
                                                                                        │
                                                                                        ▼
                                                                                    Generate DefinitionId (UUIDv4)
                                                                                        │
                                                                                        ▼
                                                                                    DefinitionPersistencePort (LLD-02)
                                                                                        │
                                                                                        │ commit_registered_definition(...)
                                                                                        │ (serialize_validated_spec with thaw_json)
                                                                                        ▼
                                                                                    PostgreSQL 16 Database
                                                                                        │
                                                                                        │ (INSERT ON CONFLICT DO NOTHING)
                                                                                        ▼
                                                                                    Return CommitOutcome
                                                                                        │
                                                                                        ▼
                                                                                    Return CREATED / IDEMPOTENT_MATCH / IDEMPOTENCY_CONFLICT
```

---

## 17. Scope Boundaries: Explicitly Prohibited Features

To protect system determinism and prevent architectural drift, the following features are strictly prohibited:
1. **Workflow Semantic Versioning:** Workflow versions (e.g., `v1.2.0`, `patch`) are prohibited (ADR-024 deferred). Each registration creates an immutable `DefinitionId`.
2. **Conditional or Expression Edges:** Edges based on runtime expressions, variables, or predicates are prohibited.
3. **Event Triggers & Cron Schedules:** External webhooks, reactive message queues, and cron expressions are not definition concepts.
4. **Dynamic Retries & Backoff Policies:** Task definitions store only `max_attempts >= 1`. Execution backoff jitter and schedules are engine runtime concerns.
5. **Priority & Worker Queues:** Definitions do not specify worker queues, affinities, or priorities. Routing is strictly activity-based (ADR-009).
6. **Task Tags & Metadata Fields:** Arbitrary user tags, descriptions, or annotations are excluded from the V1 specification.
7. **Worker Availability Checks:** Definition registration does not verify whether active workers currently advertise matching `ActivityType` capabilities.
8. **Executable Code Injection:** Definitions never execute Python code, load external plugins, or evaluate Jinja/JSONPath expressions.
9. **Implicit Dependency Inference:** Data flow does not infer execution dependencies. Consuming a task's output requires an explicit declared dependency.
10. **Silent Auto-Repair:** The parser and validator never silently normalize or repair malformed user definitions.

---

## 18. LLD-04 Handoff Contract

Upon successful definition registration, the ingestion pipeline delivers the following immutable, validated artifacts to the execution dispatch system (**LLD-04**):

1. **`ValidatedWorkflowSpec`:**
   - Authoritative task definitions with exact `TaskDefinitionId` keys.
   - Exact `ActivityType` identifiers for worker capability routing.
   - Explicit declared direct `dependencies` as frozen sets.
   - Named `input_bindings` supporting `Literal`, `WorkflowInput`, and direct `TaskOutput`.
   - Positive integer `max_attempts` ($\ge 1$).
   - Explicit named `output_bindings` supporting `WorkflowTaskOutputBinding`.
2. **`CanonicalGraph`:**
   - Sparse, bidirectional adjacency structure ($O(1)$ upstream and downstream queries).
   - Pre-verified acyclic topology established in $O(V+E)$.
3. **Persistence Authority:**
   - Guarantees that the specification stored in `registered_definitions.validated_iws` matches the exact LLD-02 JSONB schema using ordinary JSON-native Python dictionaries and lists produced by `thaw_json`.
   - Guarantees that recovery deserialization reconstructs the exact domain model fail-closed with strict structural type checking and zero type coercion.

**LLD-04 Boundary Responsibility:**
- LLD-04 evaluates runtime task eligibility by comparing completed dependency outputs against `CanonicalGraph.get_upstream_dependencies`.
- LLD-04 resolves `ActivityType` against live, heartbeat-backed `WorkerSession` capabilities.
- LLD-04 initiates task claim ownership via LLD-02 persistence transactions.
- LLD-04 does not alter or re-validate workflow definitions.

---

## 19. Final Design Validation Checklist

- [x] **Persistence Serializer Thaws Frozen JSON**: Every domain `JsonValue` passes through `thaw_json(...)` before serialization; converts `MappingProxyType` to `dict` and `tuple` to `list`.
- [x] **Serializer Output Contract**: Returns exclusively ordinary JSON-native structures (`dict` with string keys, `list`, `str`, `int`, finite `float`, `bool`, `None`); never returns `MappingProxyType`, `tuple`, domain IDs, or enums.
- [x] **Serializer Fails Closed**: Reuses frozen LLD-02 `thaw_json` contract; strictly rejects non-string keys, NaN, Infinity, and unsupported types without repair.
- [x] **Round-Trip Test Suite**: Tests rich literals (nested objects backed by `MappingProxyType`, nested arrays, booleans, finite floats, JSON null) proving serialization to ordinary dicts and lossless round-trip decoding.
- [x] **Zero Type Coercion in Recovery Decoder**: Replaced unsafe `int(...)` with strict type checks (`type(val) is int`), rejecting floats, strings, and booleans.
- [x] **Strict max_attempts Decoding**: Validates `type(raw_max_attempts) is int` (rejecting `bool`), and enforces value $\ge 1$.
- [x] **Strict Durable Container Shapes**: Validates physical types of root, `tasks`, task payloads, `dependencies`, `input_bindings`, bindings, and `output_bindings` before iteration.
- [x] **No String Masquerading as Collection**: Explicitly verifies container types before iteration (e.g., rejecting `"dependencies": "task_a"`).
- [x] **Strict Persisted Literal JSON**: Ingests durable literals through `freeze_json(...)` into domain models, rejecting non-string keys, NaN, and Infinity.
- [x] **Linear Graph & Cycle Processing**: Graph construction and Kahn cycle detection operate strictly in $O(V+E)$ with zero node sorting.
- [x] **Topological Order Separated & Non-Authoritative**: Documented deterministic topological order as an optional $O((V+E) \log V)$ convenience helper, not a semantic invariant or persistence dependency.
- [x] **Accurate Graph Complexity Table**: Distinguishes $O(V+E)$ graph/cycle detection from $O((V+E) \log V)$ deterministic topological sort.
- [x] **YAML 1.2 Scalar Handling Documented**: Clarified that under safe YAML 1.2 `true`/`false` are booleans, whereas unquoted `yes`/`on` are parsed as strings.
- [x] **Fingerprint Distinctions**: Documented that `flag: true` (bool) and `flag: "true"` (string) produce different request fingerprints.
- [x] **Parser Implementation Alignment**: Documented `ruamel.yaml` as pure-Python safe parser (`typ="safe", pure=True`).
- [x] **AST Anchor/Alias Defense**: Enforces alias/anchor rejection via composed `Node` inspection (`anchor` attribute and node type checks) with explicit tests for `&base`/`*base`.
- [x] **Comprehensive Persistence Integrity Tests**: Corrupt durable spec test cases specified for invalid types, masquerading strings, missing mappings, and relational errors.
- [x] **Preserved Frozen Semantics**: All ADR, HLD, LLD-01, and LLD-02 architectural invariants preserved.

---

### Classification

**LLD-03 Architecture-Ready / Approved as LLD-04 Input**
