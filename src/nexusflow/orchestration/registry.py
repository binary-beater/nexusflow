"""In-memory ephemeral worker registry according to LLD-05 Section 3 & LLD-01 Section 10."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from nexusflow.domain.identifiers import ActivityType, WorkerSessionId


@dataclass(frozen=True, slots=True)
class WorkerSessionRecord:
    session_id: WorkerSessionId
    worker_id: str
    capabilities: frozenset[ActivityType]
    registered_at_monotonic: float
    last_heartbeat_monotonic: float
    last_heartbeat_utc: datetime
    accepting_new_work: bool


@dataclass(frozen=True, slots=True)
class WorkerSessionSnapshot:
    session_id: WorkerSessionId
    worker_id: str
    capabilities: frozenset[ActivityType]
    live: bool
    accepting_new_work: bool
    last_heartbeat_utc: datetime


class WorkerRegistry:
    """Ephemeral, thread-safe in-memory cache of worker sessions.

    Holds ZERO durable orchestration authority (PostgreSQL is sole truth).
    Registry lock is never held across database I/O, network calls, or long CPU operations.
    """

    def __init__(self, liveness_timeout_seconds: float = 15.0) -> None:
        self._liveness_timeout_seconds = liveness_timeout_seconds
        self._sessions: dict[UUID, WorkerSessionRecord] = {}
        self._delivery_queues: dict[UUID, asyncio.Queue] = {}
        self._lock = asyncio.Lock()

    async def register(
        self,
        worker_id: str,
        capabilities: frozenset[ActivityType],
        accepting_new_work: bool = True,
    ) -> WorkerSessionRecord:
        now_monotonic = time.monotonic()
        now_utc = datetime.now(UTC)
        session_id = WorkerSessionId.generate()

        record = WorkerSessionRecord(
            session_id=session_id,
            worker_id=worker_id,
            capabilities=capabilities,
            registered_at_monotonic=now_monotonic,
            last_heartbeat_monotonic=now_monotonic,
            last_heartbeat_utc=now_utc,
            accepting_new_work=accepting_new_work,
        )

        async with self._lock:
            self._sessions[session_id.value] = record
            self._delivery_queues[session_id.value] = asyncio.Queue()

        return record

    async def heartbeat(
        self,
        session_id: WorkerSessionId,
        accepting_new_work: bool = True,
    ) -> bool:
        """Updates last_heartbeat_monotonic. Returns False if session is unknown."""
        now_monotonic = time.monotonic()
        now_utc = datetime.now(UTC)

        async with self._lock:
            record = self._sessions.get(session_id.value)
            if record is None:
                return False

            updated = WorkerSessionRecord(
                session_id=record.session_id,
                worker_id=record.worker_id,
                capabilities=record.capabilities,
                registered_at_monotonic=record.registered_at_monotonic,
                last_heartbeat_monotonic=now_monotonic,
                last_heartbeat_utc=now_utc,
                accepting_new_work=accepting_new_work,
            )
            self._sessions[session_id.value] = updated
            return True

    async def get_snapshot(self, session_id: WorkerSessionId) -> WorkerSessionSnapshot | None:
        now_monotonic = time.monotonic()
        async with self._lock:
            rec = self._sessions.get(session_id.value)
            if rec is None:
                return None
            is_live = (now_monotonic - rec.last_heartbeat_monotonic) <= self._liveness_timeout_seconds
            return WorkerSessionSnapshot(
                session_id=rec.session_id,
                worker_id=rec.worker_id,
                capabilities=rec.capabilities,
                live=is_live,
                accepting_new_work=rec.accepting_new_work,
                last_heartbeat_utc=rec.last_heartbeat_utc,
            )

    async def find_eligible_worker_for_activity(
        self,
        activity_type: ActivityType,
    ) -> WorkerSessionSnapshot | None:
        """Finds a live worker session advertising exact capability and accepting work."""
        now_monotonic = time.monotonic()
        async with self._lock:
            for rec in self._sessions.values():
                is_live = (now_monotonic - rec.last_heartbeat_monotonic) <= self._liveness_timeout_seconds
                if is_live and rec.accepting_new_work and (activity_type in rec.capabilities):
                    return WorkerSessionSnapshot(
                        session_id=rec.session_id,
                        worker_id=rec.worker_id,
                        capabilities=rec.capabilities,
                        live=True,
                        accepting_new_work=rec.accepting_new_work,
                        last_heartbeat_utc=rec.last_heartbeat_utc,
                    )
        return None

    async def queue_delivery(self, session_id: WorkerSessionId, item: object) -> bool:
        async with self._lock:
            q = self._delivery_queues.get(session_id.value)
            if q is not None:
                await q.put(item)
                return True
            return False

    async def poll_delivery(
        self,
        session_id: WorkerSessionId,
        timeout_seconds: float,
    ) -> object | None:
        async with self._lock:
            q = self._delivery_queues.get(session_id.value)
            if q is None:
                return None

        try:
            return await asyncio.wait_for(q.get(), timeout=timeout_seconds)
        except TimeoutError:
            return None
