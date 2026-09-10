"""OpenTelemetry tracing setup (fail-open) according to LLD-09 Section 10.3."""

import logging
import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

logger = logging.getLogger(__name__)

_tracer_initialized = False


def setup_tracing(service_name: str = "nexusflow-control-plane") -> trace.Tracer:
    """Initializes OpenTelemetry TracerProvider with fail-open OTLP or Console exporter."""
    global _tracer_initialized
    if _tracer_initialized:
        return trace.get_tracer(service_name)

    resource = Resource.create(attributes={"service.name": service_name})
    provider = TracerProvider(resource=resource)

    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    if otlp_endpoint:
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
            provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
            logger.info("OpenTelemetry OTLP exporter configured for endpoint: %s", otlp_endpoint)
        except Exception as exc:
            logger.warning("Failed to initialize OTLP exporter (fail-open): %s", exc)
    else:
        # Fallback to in-memory/noop or silent when OTLP is not configured
        pass

    trace.set_tracer_provider(provider)
    _tracer_initialized = True
    return trace.get_tracer(service_name)


def get_tracer(name: str = "nexusflow") -> trace.Tracer:
    """Retrieves an application tracer."""
    return trace.get_tracer(name)
