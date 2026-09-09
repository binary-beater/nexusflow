"""Deeply immutable Pydantic-Settings configuration (LLD-09 Section 3)."""

import hashlib
import hmac
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexusflow.domain.enums import PrincipalType, PublicPermission
from nexusflow.domain.security import SecurityContext


class FrozenSettingsModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DatabaseSettings(FrozenSettingsModel):
    url: str = "postgresql+asyncpg://nexusflow_user:nexusflow_password@localhost:5432/nexusflow"
    pool_size: int = 20
    max_overflow: int = 10
    pool_timeout: float = 30.0
    echo: bool = False


class HttpSettings(FrozenSettingsModel):
    host: str = "0.0.0.0"
    port: int = 8000
    workers: int = 1
    max_payload_bytes: int = 1_000_000


class SecuritySettings(FrozenSettingsModel):
    # Comma-separated or whitespace-separated list of raw public bearer tokens
    public_tokens: str = "dev-secret-token"
    # Single worker secret
    worker_token: str = "dev-worker-secret"


@dataclass(frozen=True, slots=True)
class RuntimeSecurityAuthority:
    """Stores precomputed cryptographic SHA-256 digests of valid tokens."""

    public_token_digests: frozenset[bytes]
    worker_token_digest: bytes

    @classmethod
    def from_settings(cls, settings: SecuritySettings) -> "RuntimeSecurityAuthority":
        raw_tokens = [t.strip() for t in settings.public_tokens.split(",") if t.strip()]
        public_digests = frozenset(hashlib.sha256(t.encode("utf-8")).digest() for t in raw_tokens)
        worker_digest = hashlib.sha256(settings.worker_token.encode("utf-8")).digest()
        return cls(public_token_digests=public_digests, worker_token_digest=worker_digest)

    def authenticate_public_token(self, token: str) -> SecurityContext | None:
        supplied_digest = hashlib.sha256(token.encode("utf-8")).digest()
        for expected in self.public_token_digests:
            if hmac.compare_digest(expected, supplied_digest):
                # Valid public token grants all standard public permissions
                return SecurityContext(
                    principal_id="public-client",
                    principal_type=PrincipalType.PUBLIC_CLIENT,
                    permissions=frozenset(
                        [
                            PublicPermission.DEFINITIONS_READ,
                            PublicPermission.DEFINITIONS_WRITE,
                            PublicPermission.EXECUTIONS_READ,
                            PublicPermission.EXECUTIONS_START,
                            PublicPermission.EXECUTIONS_CANCEL,
                        ]
                    ),
                )
        return None

    def authenticate_worker_token(self, token: str) -> bool:
        supplied_digest = hashlib.sha256(token.encode("utf-8")).digest()
        return hmac.compare_digest(self.worker_token_digest, supplied_digest)


class RetrySettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    fixed_delay_seconds: float = Field(default=5.0, gt=0.0)


class RecoverySettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    keyset_batch_size: int = Field(default=100, ge=1, le=1000)
    convergence_max_passes: int = Field(default=10, ge=1, le=50)


class ExecutionTimeoutSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    default_start_deadline_seconds: float = Field(default=30.0, gt=0.0)
    default_execution_timeout_seconds: float = Field(default=300.0, gt=0.0)
    cancellation_grace_seconds: float = Field(default=10.0, gt=0.0)


class NexusFlowSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="NEXUSFLOW_",
        env_nested_delimiter="__",
        extra="forbid",
        frozen=True,
    )

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    http: HttpSettings = Field(default_factory=HttpSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    recovery: RecoverySettings = Field(default_factory=RecoverySettings)
    timeouts: ExecutionTimeoutSettings = Field(default_factory=ExecutionTimeoutSettings)
