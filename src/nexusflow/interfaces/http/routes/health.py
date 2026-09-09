"""Liveness and readiness endpoints (LLD-08 Section 2.3, LLD-09)."""

from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexusflow.interfaces.http.dependencies import (
    RecoveryGateProtocol,
    get_db_session,
    get_recovery_gate,
)

router = APIRouter(tags=["Health"])


@router.get("/healthz", status_code=status.HTTP_200_OK)
async def healthz() -> dict[str, str]:
    """Basic process liveness check."""
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(
    response: Response,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    gate: Annotated[RecoveryGateProtocol, Depends(get_recovery_gate)],
) -> dict[str, str]:
    """Process readiness check: verifies DB connectivity and recovery readiness."""
    try:
        await session.execute(text("SELECT 1;"))
    except Exception as exc:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "unavailable", "reason": f"Database unreachable: {exc}"}

    if not gate.allows_new_work():
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "recovering", "reason": "Recovery not yet converged"}

    return {"status": "ready"}
