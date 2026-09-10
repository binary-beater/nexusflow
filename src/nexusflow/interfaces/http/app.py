from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from nexusflow.interfaces.http.dependencies import (
    get_scheduler,
    get_session_factory,
    get_startup_recovery_gate,
    get_worker_registry,
)
from nexusflow.interfaces.http.errors import ApiHttpException, ErrorPayload, StandardErrorEnvelope
from nexusflow.interfaces.http.routes.definitions import router as definitions_router
from nexusflow.interfaces.http.routes.health import router as health_router
from nexusflow.observability.logging import setup_logging
from nexusflow.observability.middleware import MetricsMiddleware
from nexusflow.observability.tracing import setup_tracing
from nexusflow.orchestration.recovery import StartupRecoveryEngine


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Initialize structured logging and OpenTelemetry tracing
    setup_logging()
    setup_tracing("nexusflow-control-plane")

    # Run deterministic startup recovery
    session_factory = get_session_factory()
    registry = get_worker_registry()
    scheduler = get_scheduler(session_factory, registry)
    gate = get_startup_recovery_gate()

    recovery_engine = StartupRecoveryEngine(
        session_factory=session_factory,
        scheduler=scheduler,
        worker_registry=registry,
    )
    await recovery_engine.recover_system()
    gate.mark_recovery_completed()

    yield


def create_app() -> FastAPI:
    """Creates and configures the NexusFlow V1 FastAPI application."""
    app = FastAPI(
        title="NexusFlow Control Plane",
        version="1.0.0",
        docs_url="/docs",
        redoc_url=None,
        lifespan=lifespan,
    )

    app.add_middleware(MetricsMiddleware)

    # Standardized error handlers (ADR-018, LLD-08 Section 15)
    @app.exception_handler(ApiHttpException)
    async def api_http_exception_handler(
        request: Request, exc: ApiHttpException
    ) -> JSONResponse:
        envelope = StandardErrorEnvelope(
            error=ErrorPayload(
                code=exc.code,
                message=exc.message,
                details=exc.details,
                request_id=request.headers.get("x-request-id"),
            )
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=envelope.model_dump(),
            headers=exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        envelope = StandardErrorEnvelope(
            error=ErrorPayload(
                code="STRUCTURAL_VALIDATION_FAILURE",
                message="Request payload failed schema validation.",
                details={"errors": exc.errors()},
                request_id=request.headers.get("x-request-id"),
            )
        )
        return JSONResponse(
            status_code=400,
            content=envelope.model_dump(),
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        envelope = StandardErrorEnvelope(
            error=ErrorPayload(
                code="INTERNAL_SERVER_ERROR",
                message="An unexpected internal server error occurred.",
                details={},
                request_id=request.headers.get("x-request-id"),
            )
        )
        return JSONResponse(
            status_code=500,
            content=envelope.model_dump(),
        )

    from nexusflow.interfaces.http.routes.executions import router as executions_router
    from nexusflow.interfaces.http.routes.worker import router as worker_router

    # Mount routers
    app.include_router(health_router)
    app.include_router(definitions_router)
    app.include_router(executions_router)
    app.include_router(worker_router)

    return app


app = create_app()
