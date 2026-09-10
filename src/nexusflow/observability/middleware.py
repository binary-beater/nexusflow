"""HTTP middleware for Prometheus metrics and request tracing (fail-open)."""

import time
from collections.abc import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from nexusflow.observability.metrics import HTTP_REQUEST_DURATION_SECONDS


class MetricsMiddleware(BaseHTTPMiddleware):
    """Measures HTTP request latency by route template and status code."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration = time.perf_counter() - start_time
            # Normalize route to prevent cardinality explosion
            route = request.url.path
            for r in request.app.routes:
                match, _ = r.matches(request.scope)
                if match:
                    route = getattr(r, "path", route)
                    break

            try:
                HTTP_REQUEST_DURATION_SECONDS.labels(
                    method=request.method,
                    route=route,
                    status_code=str(status_code),
                ).observe(duration)
            except Exception:
                pass
