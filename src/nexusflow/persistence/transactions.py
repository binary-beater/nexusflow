"""Durable definition persistence transactions according to LLD-02 Section 10.1."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexusflow.domain.identifiers import DefinitionId, IdempotencyKey, RequestFingerprint
from nexusflow.domain.spec import ValidatedWorkflowSpec
from nexusflow.persistence.orm import IdempotencyRecord, RegisteredDefinitionRecord


class CommitStatus(StrEnum):
    COMMITTED = "COMMITTED"
    PRECONDITION_FAILED = "PRECONDITION_FAILED"
    OCC_CONFLICT = "OCC_CONFLICT"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"


@dataclass(frozen=True, slots=True)
class CommitOutcome:
    status: CommitStatus
    message: str = ""


async def commit_registered_definition(
    session: AsyncSession,
    definition_id: DefinitionId,
    workflow_name: str,
    spec: ValidatedWorkflowSpec,
    raw_yaml: str | None,
    idempotency_key: IdempotencyKey | None,
    fingerprint: RequestFingerprint | None,
    now_utc: datetime,
) -> tuple[CommitOutcome, DefinitionId]:
    """Persists a semantically validated definition in PostgreSQL.

    Adheres strictly to LLD-02 Section 10.1:
    - Without an idempotency key: creates a distinct DefinitionId.
    - With an idempotency key:
      - same key + same fingerprint: returns existing DefinitionId.
      - same key + different fingerprint: returns PRECONDITION_FAILED.
    """
    bind = session.get_bind()
    is_sqlite = bind is not None and "sqlite" in bind.dialect.name
    insert_fn = sqlite_insert if is_sqlite else pg_insert

    async with session.begin():
        if idempotency_key is not None:
            if fingerprint is None:
                raise ValueError("Fingerprint cannot be None when idempotency_key is supplied")

            stmt_idem = (
                insert_fn(IdempotencyRecord)
                .values(
                    operation_type="REGISTER_DEFINITION",
                    idempotency_key=idempotency_key.value,
                    request_fingerprint=fingerprint.digest,
                    resource_id=definition_id.value,
                    created_at_utc=now_utc,
                )
                .on_conflict_do_nothing(index_elements=["operation_type", "idempotency_key"])
            )
            res: Any = await session.execute(stmt_idem)
            if getattr(res, "rowcount", 0) == 0:
                # Concurrent creator raced or previous registration with this key exists
                existing = await session.scalar(
                    select(IdempotencyRecord).where(
                        IdempotencyRecord.operation_type == "REGISTER_DEFINITION",
                        IdempotencyRecord.idempotency_key == idempotency_key.value,
                    )
                )
                if existing is not None and existing.request_fingerprint == fingerprint.digest:
                    return (
                        CommitOutcome(status=CommitStatus.COMMITTED, message="Idempotent match"),
                        DefinitionId(existing.resource_id),
                    )
                return (
                    CommitOutcome(
                        status=CommitStatus.PRECONDITION_FAILED,
                        message="Conflicting idempotency key",
                    ),
                    definition_id,
                )

        from nexusflow.definition.codec import serialize_validated_spec

        session.add(
            RegisteredDefinitionRecord(
                definition_id=definition_id.value,
                workflow_name=workflow_name,
                validated_iws=serialize_validated_spec(spec),
                raw_yaml=raw_yaml,
                created_at_utc=now_utc,
            )
        )

    return CommitOutcome(status=CommitStatus.COMMITTED), definition_id
