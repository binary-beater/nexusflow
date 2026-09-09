from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4


@dataclass(frozen=True, slots=True)
class DefinitionId:
    value: UUID

    @classmethod
    def generate(cls) -> DefinitionId:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True, slots=True)
class WorkflowExecutionId:
    value: UUID

    @classmethod
    def generate(cls) -> WorkflowExecutionId:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True, slots=True)
class TaskExecutionId:
    value: UUID

    @classmethod
    def generate(cls) -> TaskExecutionId:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True, slots=True)
class AttemptId:
    value: UUID

    @classmethod
    def generate(cls) -> AttemptId:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True, slots=True)
class WorkerSessionId:
    """Runtime incarnation identity of a worker process. Non-secret correlation ID."""

    value: UUID

    @classmethod
    def generate(cls) -> WorkerSessionId:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True, slots=True)
class HistoryEntryId:
    value: UUID

    @classmethod
    def generate(cls) -> HistoryEntryId:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True, slots=True)
class TaskDefinitionId:
    """Local semantic task identity within a workflow specification (e.g., 'validate_payment')."""

    value: str

    def __post_init__(self) -> None:
        if not self.value or len(self.value) > 256:
            raise ValueError("TaskDefinitionId must be a non-empty string <= 256 characters.")
        if any(c in self.value for c in "\x00\r\n\t"):
            raise ValueError("TaskDefinitionId contains prohibited control characters.")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class ActivityType:
    """Canonical activity string matched via exact byte-for-byte string equality."""

    name: str

    def __post_init__(self) -> None:
        if not self.name or len(self.name) > 256:
            raise ValueError("ActivityType must be a non-empty string <= 256 characters.")
        if any(c in self.name for c in "\x00\r\n\t"):
            raise ValueError("ActivityType contains prohibited control characters.")

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class IdempotencyKey:
    """Client-provided idempotency token."""

    value: str

    def __post_init__(self) -> None:
        if not self.value or len(self.value) > 256:
            raise ValueError("IdempotencyKey must be between 1 and 256 characters.")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class RequestFingerprint:
    """Cryptographic SHA-256 digest of normalized request payload to verify semantic equivalence."""

    digest: str

    def __post_init__(self) -> None:
        if len(self.digest) != 64:
            raise ValueError("RequestFingerprint must be a 64-character SHA-256 hex string.")

    def __str__(self) -> str:
        return self.digest
