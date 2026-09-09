"""Unit tests for ephemeral worker registry and monotonic liveness."""

import asyncio

import pytest

from nexusflow.domain.identifiers import ActivityType
from nexusflow.orchestration.registry import WorkerRegistry


@pytest.mark.asyncio
async def test_worker_registration_and_liveness():
    registry = WorkerRegistry(liveness_timeout_seconds=0.2)
    act_a = ActivityType("math.add")

    record = await registry.register(
        worker_id="w1",
        capabilities=frozenset([act_a]),
        accepting_new_work=True,
    )

    snap = await registry.get_snapshot(record.session_id)
    assert snap is not None
    assert snap.live is True
    assert snap.accepting_new_work is True
    assert act_a in snap.capabilities

    # Eligible worker search
    found = await registry.find_eligible_worker_for_activity(act_a)
    assert found is not None
    assert found.session_id == record.session_id

    # Search for missing activity returns None
    missing = await registry.find_eligible_worker_for_activity(ActivityType("missing.act"))
    assert missing is None

    # Heartbeat maintains liveness
    await asyncio.sleep(0.1)
    ok = await registry.heartbeat(record.session_id)
    assert ok is True

    # Liveness expires after timeout without heartbeat
    await asyncio.sleep(0.25)
    expired_snap = await registry.get_snapshot(record.session_id)
    assert expired_snap is not None
    assert expired_snap.live is False

    # Expired worker is not eligible
    not_found = await registry.find_eligible_worker_for_activity(act_a)
    assert not_found is None
