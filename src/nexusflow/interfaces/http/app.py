"""FastAPI Application Factory for NexusFlow V1 (LLD-08, LLD-09)."""

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from nexusflow.interfaces.http.errors import ApiHttpException, ErrorPayload, StandardErrorEnvelope
from nexusflow.interfaces.http.routes.definitions import router as definitions_router
from nexusflow.interfaces.http.routes.health import router as health_router


def create_app() -> FastAPI:
    """Creates and configures the NexusFlow V1 FastAPI application."""
    app = FastAPI(
        title="NexusFlow Control Plane",
        version="1.0.0",
        docs_url="/docs",
        redoc_url=None,
    )

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

    # Mount routers
    app.include_router(health_router)
    app.include_router(definitions_router)

    return app


app = create_app()
