"""Standard API error envelopes and HTTP exception mapping (LLD-08 Section 15)."""

from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict


class ErrorPayload(BaseModel):
    model_config = ConfigDict(frozen=True)
    code: str
    message: str
    details: dict[str, Any] | None = None
    request_id: str | None = None


class StandardErrorEnvelope(BaseModel):
    model_config = ConfigDict(frozen=True)
    error: ErrorPayload


class ApiHttpException(HTTPException):
    """Structured HTTP exception rendering standard ADR-018 envelope."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(status_code=status_code, detail=message, headers=headers)
        self.code = code
        self.message = message
        self.details = details or {}
