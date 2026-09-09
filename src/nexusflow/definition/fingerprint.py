"""Deterministic request fingerprinting for idempotency (LLD-03 Section 10)."""

import hashlib
import json
from typing import Any

from nexusflow.domain.identifiers import RequestFingerprint


def compute_registration_fingerprint(normalized_spec_dict: dict[str, Any]) -> RequestFingerprint:
    """Computes a deterministic SHA-256 RequestFingerprint from serialized canonical IWS.

    Sorts mapping keys and uses compact separators to guarantee semantic invariance
    against whitespace, comments, and YAML key ordering.
    """
    canonical_bytes = json.dumps(
        normalized_spec_dict,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")

    digest_hex = hashlib.sha256(canonical_bytes).hexdigest()
    return RequestFingerprint(digest=digest_hex)
