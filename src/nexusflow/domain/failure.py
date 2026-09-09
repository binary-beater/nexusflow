from dataclasses import dataclass

from nexusflow.domain.json_compat import JsonObject


@dataclass(frozen=True, slots=True)
class WorkerFailureReport:
    """Untrusted raw observation submitted by a worker. Worker never decides retryability."""
    error_code: str
    error_message: str
    details: JsonObject | None = None
