import os
from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexusflow.definition.codec import deserialize_validated_spec, serialize_validated_spec
from nexusflow.definition.dto import WorkflowDefinitionDTO
from nexusflow.definition.fingerprint import compute_registration_fingerprint
from nexusflow.definition.normalizer import normalize_workflow_dto
from nexusflow.definition.parser import parse_yaml_to_ast
from nexusflow.definition.validator import SemanticValidator
from nexusflow.domain.identifiers import DefinitionId, IdempotencyKey
from nexusflow.persistence.orm import RegisteredDefinitionRecord
from nexusflow.persistence.transactions import CommitStatus, commit_registered_definition

POSTGRES_TEST_URL = os.getenv(
    "NEXUSFLOW_DB__URL",
    "postgresql+asyncpg://nexusflow_user:nexusflow_password@localhost:5432/nexusflow",
)


@pytest.fixture
async def postgres_session():
    """Real PostgreSQL 16 session for integration testing of transactions and ORM."""
    engine = create_async_engine(POSTGRES_TEST_URL, echo=False)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as session:
        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_definition_persistence_roundtrip(postgres_session: AsyncSession):
    session = postgres_session
    yaml_text = """
workflow_name: order_fulfillment
tasks:
  verify:
    activity_type: inventory.verify
    dependencies: []
    input_bindings:
      order_id:
        type: workflow_input
    max_attempts: 3
"""
    ast = parse_yaml_to_ast(yaml_text)
    dto = WorkflowDefinitionDTO.model_validate(ast)
    val_outcome = SemanticValidator().validate(normalize_workflow_dto(dto))
    assert val_outcome.success is not None
    spec = val_outcome.success.spec

    def_id = DefinitionId.generate()
    now_utc = datetime.now(UTC)

    # Persist
    status, res_id = await commit_registered_definition(
        session=session,
        definition_id=def_id,
        workflow_name=spec.workflow_name,
        spec=spec,
        raw_yaml=yaml_text,
        idempotency_key=None,
        fingerprint=None,
        now_utc=now_utc,
    )
    assert status.status == CommitStatus.COMMITTED
    assert res_id == def_id

    # Read back
    record = await session.scalar(
        select(RegisteredDefinitionRecord).where(
            RegisteredDefinitionRecord.definition_id == def_id.value
        )
    )
    assert record is not None
    assert record.workflow_name == "order_fulfillment"

    # Deserialization test
    deserialized_spec = deserialize_validated_spec(record.validated_iws)
    assert deserialized_spec.workflow_name == "order_fulfillment"
    assert len(deserialized_spec.tasks) == 1


@pytest.mark.asyncio
async def test_definition_idempotency_matrix(postgres_session: AsyncSession):
    session = postgres_session
    yaml_text_1 = """
workflow_name: demo
tasks:
  step1:
    activity_type: demo.act
    dependencies: []
    max_attempts: 2
"""
    val_outcome_1 = SemanticValidator().validate(
        normalize_workflow_dto(WorkflowDefinitionDTO.model_validate(parse_yaml_to_ast(yaml_text_1)))
    )
    assert val_outcome_1.success is not None
    spec_1 = val_outcome_1.success.spec
    fp_1 = compute_registration_fingerprint(serialize_validated_spec(spec_1))

    import uuid

    test_run_id = uuid.uuid4().hex[:8]
    idem_key = IdempotencyKey(f"demo-key-{test_run_id}")
    def_id_1 = DefinitionId.generate()
    now_utc = datetime.now(UTC)

    # 1. Initial registration with idempotency key
    status_1, res_id_1 = await commit_registered_definition(
        session=session,
        definition_id=def_id_1,
        workflow_name=spec_1.workflow_name,
        spec=spec_1,
        raw_yaml=yaml_text_1,
        idempotency_key=idem_key,
        fingerprint=fp_1,
        now_utc=now_utc,
    )
    assert status_1.status == CommitStatus.COMMITTED
    assert res_id_1 == def_id_1

    # 2. Resubmission with same key and same semantics returns original DefinitionId
    def_id_2 = DefinitionId.generate()
    status_2, res_id_2 = await commit_registered_definition(
        session=session,
        definition_id=def_id_2,
        workflow_name=spec_1.workflow_name,
        spec=spec_1,
        raw_yaml=yaml_text_1,
        idempotency_key=idem_key,
        fingerprint=fp_1,
        now_utc=now_utc,
    )
    assert status_2.status == CommitStatus.COMMITTED
    assert res_id_2 == def_id_1  # Original ID returned!

    # 3. Submission with same key but different semantics returns PRECONDITION_FAILED
    yaml_text_diff = """
workflow_name: demo
tasks:
  step1:
    activity_type: demo.act
    dependencies: []
    max_attempts: 5
"""
    val_outcome_diff = SemanticValidator().validate(
        normalize_workflow_dto(
            WorkflowDefinitionDTO.model_validate(parse_yaml_to_ast(yaml_text_diff))
        )
    )
    assert val_outcome_diff.success is not None
    spec_diff = val_outcome_diff.success.spec
    fp_diff = compute_registration_fingerprint(serialize_validated_spec(spec_diff))

    def_id_3 = DefinitionId.generate()
    status_3, res_id_3 = await commit_registered_definition(
        session=session,
        definition_id=def_id_3,
        workflow_name=spec_diff.workflow_name,
        spec=spec_diff,
        raw_yaml=yaml_text_diff,
        idempotency_key=idem_key,
        fingerprint=fp_diff,
        now_utc=now_utc,
    )
    assert status_3.status == CommitStatus.PRECONDITION_FAILED

    # 4. Case D — Different key, same semantics -> new independent DefinitionId
    idem_key_d = IdempotencyKey(f"demo-key-d-{test_run_id}")
    def_id_d = DefinitionId.generate()
    status_d, res_id_d = await commit_registered_definition(
        session=session,
        definition_id=def_id_d,
        workflow_name=spec_1.workflow_name,
        spec=spec_1,
        raw_yaml=yaml_text_1,
        idempotency_key=idem_key_d,
        fingerprint=fp_1,
        now_utc=now_utc,
    )
    assert status_d.status == CommitStatus.COMMITTED
    assert res_id_d == def_id_d
    assert res_id_d != def_id_1

    # 5. Case E — No key, same semantics twice -> two independent DefinitionIds
    def_id_e1 = DefinitionId.generate()
    status_e1, res_id_e1 = await commit_registered_definition(
        session=session,
        definition_id=def_id_e1,
        workflow_name=spec_1.workflow_name,
        spec=spec_1,
        raw_yaml=yaml_text_1,
        idempotency_key=None,
        fingerprint=None,
        now_utc=now_utc,
    )
    assert status_e1.status == CommitStatus.COMMITTED
    assert res_id_e1 == def_id_e1

    def_id_e2 = DefinitionId.generate()
    status_e2, res_id_e2 = await commit_registered_definition(
        session=session,
        definition_id=def_id_e2,
        workflow_name=spec_1.workflow_name,
        spec=spec_1,
        raw_yaml=yaml_text_1,
        idempotency_key=None,
        fingerprint=None,
        now_utc=now_utc,
    )
    assert status_e2.status == CommitStatus.COMMITTED
    assert res_id_e2 == def_id_e2
    assert res_id_e1 != res_id_e2
    assert res_id_e1 != def_id_1
