"""OpenTelemetry tracing setup.

Trace context propagates THROUGH THE BROKER — injected into the TaskEnvelope
at enqueue, extracted in the worker. A task's trace spans:
  producer → broker → lease → execute → retry → DLQ

Retries and reclaims use SPAN LINKS to the prior attempt's trace,
never a fresh root trace. This is what makes a task's lifecycle
traceable across multiple worker processes and retries.
"""

from __future__ import annotations

from typing import Any

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.context.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Link, SpanKind, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from dtq.core.config import settings

_tracer: trace.Tracer | None = None
_propagator = TraceContextTextMapPropagator()


def init_tracing() -> None:
    """Initialize OpenTelemetry tracing with OTLP exporter."""
    global _tracer

    if not settings.otel_enabled:
        _tracer = trace.get_tracer("dtq", "0.1.0")
        return

    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    resource = Resource.create({
        "service.name": "dtq",
        "service.version": "0.1.0",
        "service.instance.id": settings.worker_id,
    })

    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=settings.otel_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    _tracer = trace.get_tracer("dtq", "0.1.0")


def get_tracer() -> trace.Tracer:
    """Get the configured tracer instance."""
    global _tracer
    if _tracer is None:
        _tracer = trace.get_tracer("dtq", "0.1.0")
    return _tracer


def inject_trace_context() -> dict[str, str]:
    """Inject current trace context into a carrier dict (for TaskEnvelope).

    This is called at enqueue time to propagate context through the broker.
    """
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)
    return carrier


def extract_trace_context(traceparent: str | None, tracestate: str | None) -> Context:
    """Extract trace context from TaskEnvelope fields.

    This is called in the worker to continue the trace from the producer.
    """
    carrier: dict[str, str] = {}
    if traceparent:
        carrier["traceparent"] = traceparent
    if tracestate:
        carrier["tracestate"] = tracestate
    return _propagator.extract(carrier)


def create_span_link(traceparent: str | None) -> Link | None:
    """Create a span link to a prior attempt's trace.

    Retries and reclaims link to the original attempt's trace rather than
    creating a disconnected root trace.
    """
    if not traceparent:
        return None
    carrier = {"traceparent": traceparent}
    ctx = _propagator.extract(carrier)
    span_ctx = trace.get_current_span(ctx).get_span_context()
    if span_ctx.is_valid:
        return Link(span_ctx)
    return None


# --- Convenience span creators ---


def start_enqueue_span(queue: str, task_name: str, task_id: str) -> Any:
    """Start a span for task enqueue."""
    tracer = get_tracer()
    return tracer.start_span(
        "enqueue",
        kind=SpanKind.PRODUCER,
        attributes={
            "dtq.queue": queue,
            "dtq.task_name": task_name,
            "dtq.task_id": task_id,
        },
    )


def start_execute_span(
    task_id: str,
    task_name: str,
    attempt: int,
    fence: int,
    worker_id: str,
    parent_context: Context | None = None,
    links: list[Link] | None = None,
) -> Any:
    """Start a span for task execution."""
    tracer = get_tracer()
    ctx = parent_context or otel_context.get_current()
    return tracer.start_span(
        "execute",
        context=ctx,
        kind=SpanKind.CONSUMER,
        links=links or [],
        attributes={
            "dtq.task_id": task_id,
            "dtq.task_name": task_name,
            "dtq.attempt": attempt,
            "dtq.fence": fence,
            "dtq.worker_id": worker_id,
        },
    )


def start_lease_span(task_id: str, worker_id: str) -> Any:
    """Start a span for lease acquisition."""
    tracer = get_tracer()
    return tracer.start_span(
        "lease",
        kind=SpanKind.CLIENT,
        attributes={"dtq.task_id": task_id, "dtq.worker_id": worker_id},
    )


def start_commit_span(task_id: str, dedup_key: str, fence: int) -> Any:
    """Start a span for effect commit."""
    tracer = get_tracer()
    return tracer.start_span(
        "commit",
        kind=SpanKind.CLIENT,
        attributes={
            "dtq.task_id": task_id,
            "dtq.dedup_key": dedup_key,
            "dtq.fence": fence,
        },
    )


def start_reclaim_span(task_id: str, worker_id: str, idle_ms: int) -> Any:
    """Start a span for task reclaim."""
    tracer = get_tracer()
    return tracer.start_span(
        "reclaim",
        kind=SpanKind.CONSUMER,
        attributes={
            "dtq.task_id": task_id,
            "dtq.worker_id": worker_id,
            "dtq.idle_ms": idle_ms,
        },
    )


def start_schedule_span(task_id: str, queue: str, delay_s: float) -> Any:
    """Start a span for scheduling a retry."""
    tracer = get_tracer()
    return tracer.start_span(
        "schedule",
        kind=SpanKind.PRODUCER,
        attributes={
            "dtq.task_id": task_id,
            "dtq.queue": queue,
            "dtq.delay_s": delay_s,
        },
    )
