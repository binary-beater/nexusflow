"""Schema verification against expected migration head (LLD-02 Section 14, LLD-09)."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


async def verify_schema_compatibility(engine: AsyncEngine, expected_version: str) -> None:
    """Verifies that the database schema matches the expected Alembic revision."""
    async with engine.connect() as conn:
        result = await conn.scalar(text("SELECT version_num FROM alembic_version LIMIT 1;"))
        if result != expected_version:
            raise RuntimeError(
                f"Database schema mismatch: expected revision {expected_version}, found {result}"
            )
