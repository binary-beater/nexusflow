"""Public definition registration and retrieval routes (LLD-08 Section 5, Section 10)."""

import json
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Request, status
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexusflow.definition.codec import serialize_validated_spec
from nexusflow.definition.dto import WorkflowDefinitionDTO
from nexusflow.definition.fingerprint import compute_registration_fingerprint
from nexusflow.definition.normalizer import normalize_workflow_dto
from nexusflow.definition.parser import YamlComplexityError, YamlSyntaxError, parse_yaml_to_ast
from nexusflow.definition.validator import SemanticValidator
from nexusflow.domain.enums import PublicPermission
from nexusflow.domain.identifiers import DefinitionId, IdempotencyKey
from nexusflow.interfaces.http.dependencies import (
    RecoveryGateProtocol,
    get_db_session,
    get_recovery_gate,
)
from nexusflow.interfaces.http.dto import DefinitionResponseDTO, RegisterDefinitionJsonDTO
from nexusflow.interfaces.http.errors import ApiHttpException
from nexusflow.interfaces.http.security import require_permission
from nexusflow.persistence.orm import RegisteredDefinitionRecord
from nexusflow.persistence.transactions import CommitStatus, commit_registered_definition

router = APIRouter(prefix="/v1/definitions", tags=["Definitions"])


@router.post("", status_code=status.HTTP_201_CREATED, response_model=DefinitionResponseDTO)
async def register_definition(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    gate: Annotated[RecoveryGateProtocol, Depends(get_recovery_gate)],
    _ctx: Annotated[object, Depends(require_permission(PublicPermission.DEFINITIONS_WRITE))],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> DefinitionResponseDTO:
    """Ingests, parses, validates, and persists a workflow definition (LLD-03, LLD-08)."""
    if not gate.allows_new_work():
        raise ApiHttpException(
            status_code=503,
            code="NOT_READY",
            message="System is currently recovering; cannot accept new workflow definitions.",
        )

    content_type = request.headers.get("content-type", "").lower()
    raw_body = await request.body()

    # Determine raw YAML content
    raw_yaml: str
    if "application/json" in content_type:
        try:
            json_payload = json.loads(raw_body)
            wrapper_dto = RegisterDefinitionJsonDTO.model_validate(json_payload)
            raw_yaml = wrapper_dto.yaml_content
        except ValidationError as exc:
            raise ApiHttpException(
                status_code=400,
                code="STRUCTURAL_VALIDATION_FAILURE",
                message="Malformed JSON wrapper for definition registration.",
                details={"validation_errors": exc.errors()},
            ) from exc
        except Exception as exc:
            raise ApiHttpException(
                status_code=400,
                code="INVALID_JSON",
                message=f"Failed to parse JSON request body: {exc}",
            ) from exc
    elif "yaml" in content_type or not content_type:
        try:
            raw_yaml = raw_body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ApiHttpException(
                status_code=400,
                code="INVALID_ENCODING",
                message="Request body must be valid UTF-8 encoded YAML text.",
            ) from exc
    else:
        raise ApiHttpException(
            status_code=415,
            code="UNSUPPORTED_MEDIA_TYPE",
            message=f"Unsupported Content-Type '{content_type}'. Must be application/yaml or application/json.",
        )

    # 1. Parse YAML into Python dict with complexity checks
    try:
        parsed_dict = parse_yaml_to_ast(raw_yaml)
    except YamlComplexityError as exc:
        raise ApiHttpException(
            status_code=422,
            code="YAML_STRUCTURE_INVALID",
            message=str(exc),
        ) from exc
    except YamlSyntaxError as exc:
        raise ApiHttpException(
            status_code=400,
            code="MALFORMED_YAML",
            message=str(exc),
        ) from exc

    # 2. Structural validation via strict Pydantic DTO (extra='forbid')
    try:
        dto = WorkflowDefinitionDTO.model_validate(parsed_dict)
    except ValidationError as exc:
        raise ApiHttpException(
            status_code=422,
            code="VALIDATION_ERROR",
            message="Workflow definition failed structural validation.",
            details={"validation_errors": exc.errors()},
        ) from exc

    # 3. Normalize into CandidateWorkflowSpec
    candidate_spec = normalize_workflow_dto(dto)

    # 4. Pure domain semantic validation (ADR-004)
    validator = SemanticValidator()
    outcome = validator.validate(candidate_spec)
    if outcome.success is None or outcome.errors:
        err_list = [
            {"code": e.code.value, "message": e.message, "path": e.path} for e in outcome.errors
        ]
        raise ApiHttpException(
            status_code=422,
            code="DEFINITION_VALIDATION_FAILED",
            message="Workflow definition failed semantic graph validation.",
            details={"semantic_errors": err_list},
        )

    validated_result = outcome.success
    validated_spec = validated_result.spec

    # 5. Compute deterministic RequestFingerprint
    canonical_dict = serialize_validated_spec(validated_spec)
    fingerprint = compute_registration_fingerprint(canonical_dict)

    # 6. Commit to PostgreSQL
    now_utc = datetime.now(UTC)
    new_def_id = DefinitionId.generate()
    typed_idem_key = IdempotencyKey(idempotency_key) if idempotency_key is not None else None

    commit_res, resulting_id = await commit_registered_definition(
        session=session,
        definition_id=new_def_id,
        workflow_name=validated_spec.workflow_name,
        spec=validated_spec,
        raw_yaml=raw_yaml,
        idempotency_key=typed_idem_key,
        fingerprint=fingerprint if typed_idem_key is not None else None,
        now_utc=now_utc,
    )

    if commit_res.status == CommitStatus.PRECONDITION_FAILED:
        raise ApiHttpException(
            status_code=409,
            code="IDEMPOTENCY_CONFLICT",
            message="Idempotency key was previously used with different definition semantics.",
        )

    return DefinitionResponseDTO(
        definition_id=resulting_id.value,
        workflow_name=validated_spec.workflow_name,
        spec_version="v1",
        created_at_utc=now_utc,
    )


@router.get(
    "/{definition_id}", status_code=status.HTTP_200_OK, response_model=DefinitionResponseDTO
)
async def get_definition(
    definition_id: UUID,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    _ctx: Annotated[object, Depends(require_permission(PublicPermission.DEFINITIONS_READ))],
) -> DefinitionResponseDTO:
    """Retrieves an immutable registered definition by ID (LLD-08)."""
    record = await session.scalar(
        select(RegisteredDefinitionRecord).where(
            RegisteredDefinitionRecord.definition_id == definition_id
        )
    )
    if record is None:
        raise ApiHttpException(
            status_code=404,
            code="DEFINITION_NOT_FOUND",
            message=f"Definition '{definition_id}' was not found.",
        )

    return DefinitionResponseDTO(
        definition_id=record.definition_id,
        workflow_name=record.workflow_name,
        spec_version="v1",
        created_at_utc=record.created_at_utc,
    )
