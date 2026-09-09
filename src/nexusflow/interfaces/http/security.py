"""Security dependencies and authentication guards (LLD-08 Sections 10-12)."""

from typing import Annotated

from fastapi import Depends, Header

from nexusflow.config.settings import NexusFlowSettings, RuntimeSecurityAuthority
from nexusflow.domain.enums import PublicPermission
from nexusflow.domain.security import SecurityContext
from nexusflow.interfaces.http.errors import ApiHttpException


def get_security_authority() -> RuntimeSecurityAuthority:
    """Returns singleton RuntimeSecurityAuthority initialized from settings."""
    settings = NexusFlowSettings()
    return RuntimeSecurityAuthority.from_settings(settings.security)


def get_public_security_context(
    authorization: Annotated[str | None, Header()] = None,
    authority: Annotated[RuntimeSecurityAuthority, Depends(get_security_authority)] = None,  # type: ignore
) -> SecurityContext:
    """Extracts and verifies Bearer token against public credentials domain."""
    if not authorization:
        raise ApiHttpException(
            status_code=401,
            code="UNAUTHORIZED",
            message="Missing Authorization header.",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )

    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise ApiHttpException(
            status_code=401,
            code="UNAUTHORIZED",
            message="Invalid Authorization header format. Expected 'Bearer <token>'.",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )

    token = parts[1]
    ctx = authority.authenticate_public_token(token)
    if ctx is None:
        raise ApiHttpException(
            status_code=401,
            code="UNAUTHORIZED",
            message="Invalid Bearer credential.",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )

    return ctx


def require_permission(required: PublicPermission):
    """Enforces specific domain permission check on authenticated SecurityContext."""

    def dependency(
        ctx: Annotated[SecurityContext, Depends(get_public_security_context)],
    ) -> SecurityContext:
        if not ctx.has_permission(required):
            raise ApiHttpException(
                status_code=403,
                code="FORBIDDEN",
                message=f"Principal lacks required permission: {required.value}.",
            )
        return ctx

    return dependency


def require_worker_domain_auth(
    authorization: Annotated[str | None, Header()] = None,
    authority: Annotated[RuntimeSecurityAuthority, Depends(get_security_authority)] = None,  # type: ignore
) -> bool:
    """Verifies that the request carries a valid worker-domain Bearer token (LLD-05 Section 2.2)."""
    if not authorization:
        raise ApiHttpException(
            status_code=401,
            code="UNAUTHORIZED",
            message="Missing Authorization header.",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )

    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise ApiHttpException(
            status_code=401,
            code="UNAUTHORIZED",
            message="Invalid Authorization header format. Expected 'Bearer <token>'.",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )

    token = parts[1]
    if not authority.authenticate_worker_token(token):
        raise ApiHttpException(
            status_code=401,
            code="UNAUTHORIZED",
            message="Invalid worker-domain Bearer credential.",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )

    return True
