"""HTTP application dependencies (LLD-08, LLD-09)."""

from collections.abc import AsyncGenerator
from typing import Annotated, Protocol

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexusflow.config.settings import NexusFlowSettings
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


class SimpleRecoveryGate:
    """Phase 1 RecoveryGate implementation.

    In Phase 1, recovery engine is not yet running, so we default allows_new_work to True
    to enable definition registration, while honoring the protocol.
    """

    def is_recovery_complete(self) -> bool:
        return True

    def allows_new_work(self) -> bool:
        return True

    def allows_existing_settlement(self) -> bool:
        return True


_recovery_gate = SimpleRecoveryGate()


def get_recovery_gate() -> RecoveryGateProtocol:
    return _recovery_gate
