from __future__ import annotations

import math
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

type JsonPrimitive = None | bool | int | float | str
type JsonArray = tuple["JsonValue", ...]
type JsonObject = Mapping[str, "JsonValue"]
type JsonValue = JsonPrimitive | JsonArray | JsonObject

def freeze_json(value: Any) -> JsonValue:
    """Recursively canonicalizes and deeply freezes an arbitrary JSON-compatible structure

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
            raise ValueError("NaN and Infinity are not valid JSON numbers.")
        return value
    elif isinstance(value, str):
        return value
    elif isinstance(value, (list, tuple)):
        return tuple(freeze_json(item) for item in value)
    elif isinstance(value, (dict, MappingProxyType)):
        frozen_dict: dict[str, JsonValue] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise TypeError(f"Object keys must be strings, got: {type(k).__name__}")
            frozen_dict[k] = freeze_json(v)
        return MappingProxyType(frozen_dict)
    else:
        raise TypeError(f"Type {type(value).__name__} is not JSON-compatible.")

def thaw_json(val: Any) -> Any:
    """Recursively converts frozen domain JSON into mutable dict/list for DB/API serialization.

    Fails closed: strictly rejects non-string keys and non-finite floats (NaN/Infinity).
    Never converts non-string keys to strings via str(k).
    """
    if val is None or isinstance(val, (int, str, bool)):
        return val
    if isinstance(val, float):
        if not math.isfinite(val):
            raise ValueError(f"Non-finite float value {val} is not valid JSON")
        return val
    if isinstance(val, (tuple, list)):
        return [thaw_json(x) for x in val]
    if isinstance(val, (MappingProxyType, dict)):
        result: dict[str, Any] = {}
        for k, v in val.items():
            if not isinstance(k, str):
                raise TypeError(f"JSON object keys must be strings, found: {type(k).__name__}")
            result[k] = thaw_json(v)
        return result
    raise TypeError(f"Unsupported domain JSON type for thawing: {type(val)}")
