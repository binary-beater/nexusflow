"""HTTP application dependencies (LLD-08, LLD-09)."""

from collections.abc import AsyncGenerator
from typing import Annotated, Protocol

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexusflow.config.settings import NexusFlowSettings
from nexusflow.orchestration.registry import WorkerRegistry
from nexusflow.orchestration.scheduler import ExecutionScheduler
from nexusflow.persistence.engine import create_engine_and_session_factory

# Global engine and session factory instance for the process
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        settings = NexusFlowSettings()
        _, _session_factory = create_engine_and_session_factory(
            database_url=settings.database.url,
            pool_size=settings.database.pool_size,
            max_overflow=settings.database.max_overflow,
            pool_timeout=settings.database.pool_timeout,
            echo=settings.database.echo,
        )
    return _session_factory


async def get_db_session(
    factory: Annotated[async_sessionmaker[AsyncSession], Depends(get_session_factory)],
) -> AsyncGenerator[AsyncSession, None]:
    async with factory() as session:
        yield session


class RecoveryGateProtocol(Protocol):
    def is_recovery_complete(self) -> bool: ...
    def allows_new_work(self) -> bool: ...
    def allows_existing_settlement(self) -> bool: ...


class StartupRecoveryGate:
    """Startup recovery gate controlling traffic admission during startup reconciliation."""

    def __init__(self, completed: bool = True) -> None:
        self._recovery_completed = completed

    def mark_recovery_completed(self) -> None:
        self._recovery_completed = True

    def mark_recovery_started(self) -> None:
        self._recovery_completed = False

    def is_recovery_complete(self) -> bool:
        return self._recovery_completed

    def allows_new_work(self) -> bool:
        return self._recovery_completed

    def allows_existing_settlement(self) -> bool:
        return True


SimpleRecoveryGate = StartupRecoveryGate
_recovery_gate = StartupRecoveryGate(completed=True)


def get_recovery_gate() -> RecoveryGateProtocol:
    return _recovery_gate


def get_startup_recovery_gate() -> StartupRecoveryGate:
    return _recovery_gate


# Worker Registry & Scheduler Singletons
_worker_registry: WorkerRegistry | None = None
_scheduler: ExecutionScheduler | None = None


def get_worker_registry() -> WorkerRegistry:
    global _worker_registry
    if _worker_registry is None:
        _worker_registry = WorkerRegistry()
    return _worker_registry


def get_scheduler(
    session_factory: Annotated[async_sessionmaker[AsyncSession], Depends(get_session_factory)],
    registry: Annotated[WorkerRegistry, Depends(get_worker_registry)],
) -> ExecutionScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = ExecutionScheduler(
            session_factory=session_factory,
            worker_registry=registry,
        )
    return _scheduler
