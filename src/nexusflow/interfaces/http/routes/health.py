from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexusflow.interfaces.http.dependencies import (
    RecoveryGateProtocol,
    get_db_session,
    get_recovery_gate,
    get_scheduler,
)
from nexusflow.orchestration.scheduler import ExecutionScheduler

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
    scheduler: Annotated[ExecutionScheduler, Depends(get_scheduler)],
) -> dict[str, str]:
    """Process readiness check: verifies DB connectivity, schema compatibility, recovery readiness, and scheduler availability."""
    # 1. Verify Database connectivity and schema/alembic table presence
    try:
        await session.execute(text("SELECT 1;"))
        # Verify schema table presence
        await session.execute(text("SELECT count(*) FROM registered_definitions;"))
    except Exception as exc:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "unavailable",
            "code": "DB_UNAVAILABLE",
            "reason": f"Database unreachable or unmigrated: {exc}",
        }

    # 2. Verify RecoveryGate allows work
    if not gate.allows_new_work():
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "unavailable",
            "code": "NOT_READY",
            "reason": "Recovery not yet converged",
        }

    # 3. Verify critical Phase-2 runtime / scheduler availability
    if scheduler is None:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "unavailable",
            "code": "RUNTIME_UNHEALTHY",
            "reason": "Scheduler runtime unavailable",
        }

    return {"status": "ready"}


@router.get("/metrics")
async def metrics() -> Response:
    """Prometheus exposition endpoint according to LLD-09 Section 10.2."""
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
