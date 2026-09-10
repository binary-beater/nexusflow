"""Structured JSON logging and pre-serialization redaction according to LLD-09 Section 10.1."""

import json
import logging
from datetime import UTC, datetime
from typing import Any

# Allowed non-sensitive metadata fields in structured output
ALLOWED_FIELDS = {
    "timestamp",
    "level",
    "logger",
    "message",
    "event_name",
    "request_id",
    "trace_id",
    "span_id",
    "workflow_execution_id",
    "task_execution_id",
    "attempt_id",
    "worker_session_id",
    "definition_id",
    "activity_type",
    "state",
    "operation",
    "failure_category",
    "duration_ms",
    "exception",
}

# Substring keywords that trigger message/traceback redaction if detected
REDACTED_KEYS = {
    "authorization",
    "bearer",
    "token",
    "password",
    "secret",
    "raw_yaml",
    "workflow_input",
    "workflow_output",
    "task_input",
    "task_output",
    "dsn",
}


class SanitizedJsonFormatter(logging.Formatter):
    """Formats log records as structured single-line JSON with redaction guards."""

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        lowered_msg = msg.lower()
        for key in REDACTED_KEYS:
            if key in lowered_msg and any(
                delim in lowered_msg
                for delim in ("bearer", "password=", "secret=", "token=", "dsn=")
            ):
                msg = "[MESSAGE REDACTED - CONTAINS SENSITIVE CREDENTIAL]"
                break

        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": msg,
            "event_name": getattr(record, "event_name", "UNSPECIFIED"),
            "request_id": getattr(record, "request_id", None),
            "trace_id": getattr(record, "trace_id", None),
            "span_id": getattr(record, "span_id", None),
            "workflow_execution_id": getattr(record, "workflow_execution_id", None),
            "task_execution_id": getattr(record, "task_execution_id", None),
            "attempt_id": getattr(record, "attempt_id", None),
            "worker_session_id": getattr(record, "worker_session_id", None),
            "definition_id": getattr(record, "definition_id", None),
            "activity_type": getattr(record, "activity_type", None),
            "state": getattr(record, "state", None),
            "operation": getattr(record, "operation", None),
            "failure_category": getattr(record, "failure_category", None),
            "duration_ms": getattr(record, "duration_ms", None),
        }

        if record.exc_info:
            exc_str = self.formatException(record.exc_info)
            for key in REDACTED_KEYS:
                if key in exc_str.lower():
                    exc_str = "[EXCEPTION TRACEBACK REDACTED - SENSITIVE VALUE DETECTED]"
                    break
            payload["exception"] = exc_str

        # Filter strictly to allowlisted non-null fields
        clean_payload = {k: v for k, v in payload.items() if k in ALLOWED_FIELDS and v is not None}
        return json.dumps(clean_payload)


def setup_logging(log_level: str = "INFO") -> None:
    """Configures the root logger with SanitizedJsonFormatter on stdout."""
    root_logger = logging.getLogger()
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    root_logger.setLevel(numeric_level)

    # Avoid duplicate handlers
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(SanitizedJsonFormatter())
    root_logger.addHandler(stream_handler)
