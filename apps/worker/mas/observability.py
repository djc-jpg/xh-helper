from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

try:
    from opentelemetry import trace
except ImportError:  # pragma: no cover - fallback for minimal test environments
    class _NoopSpan:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def set_attribute(self, *_args, **_kwargs) -> None:
            return None

    class _NoopTracer:
        def start_as_current_span(self, _name: str) -> _NoopSpan:
            return _NoopSpan()

    class _NoopTraceModule:
        @staticmethod
        def get_tracer(_name: str) -> _NoopTracer:
            return _NoopTracer()

    trace = _NoopTraceModule()

try:
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY
except ImportError:  # pragma: no cover - fallback for minimal test environments
    class _NoopMetric:
        def labels(self, **_kwargs):
            return self

        def inc(self, _amount: float = 1.0) -> None:
            return None

        def dec(self, _amount: float = 1.0) -> None:
            return None

        @contextmanager
        def time(self):
            yield None

    class _NoopRegistry:
        _names_to_collectors: dict[str, Any] = {}

    class Counter(_NoopMetric):  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs) -> None:
            _ = (args, kwargs)

    class Gauge(_NoopMetric):  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs) -> None:
            _ = (args, kwargs)

    class Histogram(_NoopMetric):  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs) -> None:
            _ = (args, kwargs)

    REGISTRY = _NoopRegistry()  # type: ignore[assignment]


def _get_or_create_counter(name: str, documentation: str, labelnames: tuple[str, ...] = ()) -> Counter:
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if isinstance(existing, Counter):
        return existing
    try:
        return Counter(name, documentation, labelnames=labelnames)
    except ValueError:
        existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
        if isinstance(existing, Counter):
            return existing
        raise


def _get_or_create_gauge(name: str, documentation: str, labelnames: tuple[str, ...] = ()) -> Gauge:
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if isinstance(existing, Gauge):
        return existing
    try:
        return Gauge(name, documentation, labelnames=labelnames)
    except ValueError:
        existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
        if isinstance(existing, Gauge):
            return existing
        raise


def _get_or_create_histogram(name: str, documentation: str, labelnames: tuple[str, ...] = ()) -> Histogram:
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if isinstance(existing, Histogram):
        return existing
    try:
        return Histogram(name, documentation, labelnames=labelnames)
    except ValueError:
        existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
        if isinstance(existing, Histogram):
            return existing
        raise


agent_decisions_total = _get_or_create_counter(
    "mas_agent_decisions_total",
    "MAS agent decisions",
    labelnames=("agent_id", "decision"),
)
agent_execution_total = _get_or_create_counter(
    "mas_agent_execution_total",
    "MAS agent execution outcomes",
    labelnames=("agent_id", "outcome"),
)
agent_retries_total = _get_or_create_counter(
    "mas_agent_retries_total",
    "MAS agent retry events",
    labelnames=("agent_id", "failure_type"),
)
agent_inflight = _get_or_create_gauge(
    "mas_agent_inflight",
    "MAS inflight task count",
    labelnames=("agent_id",),
)
agent_step_latency_seconds = _get_or_create_histogram(
    "mas_agent_step_latency_seconds",
    "MAS step duration in seconds",
    labelnames=("agent_id", "step"),
)


class AgentTelemetry:
    def __init__(self, *, tracer_name: str = "worker.mas") -> None:
        self._tracer = trace.get_tracer(tracer_name)

    @contextmanager
    def span(self, *, agent_id: str, step: str, attrs: dict[str, Any] | None = None) -> Iterator[Any]:
        with self._tracer.start_as_current_span(f"mas.{agent_id}.{step}") as span:
            span.set_attribute("mas.agent_id", agent_id)
            span.set_attribute("mas.step", step)
            if attrs:
                for key, value in attrs.items():
                    span.set_attribute(f"mas.{key}", value)
            with agent_step_latency_seconds.labels(agent_id=agent_id, step=step).time():
                yield span

    def record_decision(self, *, agent_id: str, decision: str) -> None:
        agent_decisions_total.labels(agent_id=agent_id, decision=decision).inc()

    def record_execution(self, *, agent_id: str, outcome: str) -> None:
        agent_execution_total.labels(agent_id=agent_id, outcome=outcome).inc()

    def record_retry(self, *, agent_id: str, failure_type: str) -> None:
        agent_retries_total.labels(agent_id=agent_id, failure_type=failure_type).inc()

    def inc_inflight(self, *, agent_id: str) -> None:
        agent_inflight.labels(agent_id=agent_id).inc()

    def dec_inflight(self, *, agent_id: str) -> None:
        agent_inflight.labels(agent_id=agent_id).dec()
