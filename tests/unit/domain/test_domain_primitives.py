from types import MappingProxyType

import pytest

from nexusflow.domain.enums import AttemptState, TaskState, WorkflowState
from nexusflow.domain.json_compat import freeze_json, thaw_json


def test_lifecycle_enums():
    assert [s.value for s in WorkflowState] == [
        "INITIALIZING",
        "RUNNING",
        "FAILING",
        "CANCELLING",
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
    ]
    assert [s.value for s in TaskState] == [
        "PENDING",
        "RUNNABLE",
        "RUNNING",
        "RETRY_WAIT",
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
    ]
    assert [s.value for s in AttemptState] == [
        "CLAIMED",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
    ]


def test_freeze_json_primitives_and_collections():
    data = {
        "str": "hello",
        "int": 42,
        "float": 3.14,
        "bool": True,
        "null": None,
        "list": [1, "two", {"nested": False}],
        "dict": {"a": 1, "b": [None]},
    }
    frozen = freeze_json(data)
    assert isinstance(frozen, MappingProxyType)
    assert isinstance(frozen["list"], tuple)
    assert isinstance(frozen["list"][2], MappingProxyType)
    assert frozen["null"] is None


def test_freeze_json_rejects_non_string_keys():
    with pytest.raises(TypeError, match="Object keys must be strings"):
        freeze_json({1: "integer_key"})

    with pytest.raises(TypeError, match="Object keys must be strings"):
        freeze_json({None: "none_key"})


def test_freeze_json_rejects_nan_and_inf():
    with pytest.raises(ValueError, match="NaN and Infinity"):
        freeze_json({"val": float("nan")})

    with pytest.raises(ValueError, match="NaN and Infinity"):
        freeze_json({"val": float("inf")})


def test_thaw_json_round_trip():
    original = {
        "name": "order",
        "items": [1, 2, 3],
        "meta": {"verified": True, "note": None},
    }
    frozen = freeze_json(original)
    thawed = thaw_json(frozen)
    assert thawed == original
    assert isinstance(thawed, dict)
    assert isinstance(thawed["items"], list)
    assert isinstance(thawed["meta"], dict)


def test_thaw_json_rejects_invalid_keys():
    with pytest.raises(TypeError, match="must be strings"):
        thaw_json({1: "bad"})


def test_security_authority_bootstrap_and_compare_digest():
    import hashlib

    from nexusflow.config.settings import RuntimeSecurityAuthority, SecuritySettings
    from nexusflow.domain.enums import PublicPermission

    raw_public = "secret-token-123"
    raw_worker = "worker-secret-456"
    expected_public_hash = hashlib.sha256(raw_public.encode("utf-8")).digest()
    expected_worker_hash = hashlib.sha256(raw_worker.encode("utf-8")).digest()

    settings = SecuritySettings(public_tokens=raw_public, worker_token=raw_worker)
    authority = RuntimeSecurityAuthority.from_settings(settings)

    # Verify authority stores SHA-256 digests, NOT raw tokens
    assert expected_public_hash in authority.public_token_digests
    assert authority.worker_token_digest == expected_worker_hash

    # Authenticate valid public token
    ctx = authority.authenticate_public_token(raw_public)
    assert ctx is not None
    assert ctx.has_permission(PublicPermission.DEFINITIONS_READ)
    assert ctx.has_permission(PublicPermission.DEFINITIONS_WRITE)

    # Authenticate invalid public token
    assert authority.authenticate_public_token("wrong-token") is None

    # Authenticate worker token
    assert authority.authenticate_worker_token(raw_worker) is True
    assert authority.authenticate_worker_token("wrong-worker-token") is False
