"""SQLAlchemy Async Engine and Session factory for NexusFlow."""


from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def create_engine_and_session_factory(
    database_url: str,
    pool_size: int = 20,
    max_overflow: int = 10,
    pool_timeout: float = 30.0,
    echo: bool = False,
) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    """Creates an AsyncEngine and async session factory with READ COMMITTED semantics."""
    engine = create_async_engine(
        database_url,
        echo=echo,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_pre_ping=True,
        isolation_level="READ COMMITTED",
    )
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    return engine, session_factory
