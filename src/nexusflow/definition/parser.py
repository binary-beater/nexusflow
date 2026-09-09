"""Safe YAML parser with AST depth, node count, and anchor/alias rejection (LLD-03)."""

from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.nodes import MappingNode, Node, SequenceNode


class YamlComplexityError(Exception):
    """Raised when YAML structure exceeds operational safety thresholds."""


class YamlSyntaxError(Exception):
    """Raised when YAML syntax is malformed or invalid."""


def create_safe_yaml_parser() -> YAML:
    """Constructs a hardened, pure-Python safe ruamel.yaml parser.

    Disallows executable constructors, custom tags, and duplicate mapping keys.
    """
    yaml = YAML(typ="safe", pure=True)
    yaml.allow_duplicate_keys = False
    return yaml


def enforce_yaml_ast_complexity(
    node: Node,
    max_depth: int = 16,
    max_nodes: int = 1000,
    allow_aliases: bool = False,
) -> None:
    """Traverses the ruamel.yaml composed Node tree to verify tree depth,

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
            if getattr(current_node, "anchor", None) is not None:
                raise YamlComplexityError("YAML anchors and aliases are prohibited in workflow definitions.")
            node_type_name = type(current_node).__name__
            if "Alias" in node_type_name or getattr(current_node, "is_alias", False):
                raise YamlComplexityError("YAML anchors and aliases are prohibited in workflow definitions.")

        if isinstance(current_node, MappingNode):
            for key_node, value_node in current_node.value:
                _traverse(key_node, current_depth + 1)
                _traverse(value_node, current_depth + 1)
        elif isinstance(current_node, SequenceNode):
            for item_node in current_node.value:
                _traverse(item_node, current_depth + 1)

    _traverse(node, 1)


def parse_yaml_to_ast(
    yaml_text: str,
    max_bytes: int = 1_000_000,
    max_depth: int = 16,
    max_nodes: int = 1000,
) -> dict[str, Any]:
    """Parses untrusted YAML string into a Python dict following LLD-03 rules.

    Rejects:
    - Payload exceeding max_bytes
    - Non-mapping roots
    - Complexity limits (depth, nodes)
    - Anchors / aliases
    - Duplicate keys
    """
    raw_bytes = yaml_text.encode("utf-8")
    if len(raw_bytes) > max_bytes:
        raise YamlComplexityError(f"YAML payload size ({len(raw_bytes)} bytes) exceeds limit of {max_bytes} bytes.")

    parser = create_safe_yaml_parser()

    try:
        composed_node = parser.compose(yaml_text)
    except Exception as exc:
        raise YamlSyntaxError(f"Malformed YAML: {exc}") from exc

    if composed_node is None:
        raise YamlSyntaxError("YAML document is empty or contains no content.")

    if not isinstance(composed_node, MappingNode):
        raise YamlSyntaxError(f"Root YAML element must be a mapping/object, got {type(composed_node).__name__}.")

    enforce_yaml_ast_complexity(composed_node, max_depth=max_depth, max_nodes=max_nodes, allow_aliases=False)

    try:
        parsed_data = parser.load(yaml_text)
    except Exception as exc:
        raise YamlSyntaxError(f"Failed to load YAML: {exc}") from exc

    if not isinstance(parsed_data, dict):
        raise YamlSyntaxError(f"Root YAML element must be a dictionary, got {type(parsed_data).__name__}.")

    return parsed_data
