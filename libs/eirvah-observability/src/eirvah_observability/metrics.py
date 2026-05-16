"""Prometheus metric factories with a uniform ``eirvah_`` namespace.

Every metric in EirVah is created through one of these factories so prefix,
default buckets, and registry handling are consistent across services.
"""

from __future__ import annotations

from collections.abc import Sequence

from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.registry import CollectorRegistry

EIRVAH_METRIC_PREFIX = "eirvah"

# Latency buckets cover sub-ms NATS hops through to multi-second timeouts.
DEFAULT_LATENCY_BUCKETS: tuple[float, ...] = (
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)


def _full_name(name: str) -> str:
    if name.startswith(f"{EIRVAH_METRIC_PREFIX}_"):
        raise ValueError(
            f"metric name {name!r} already starts with {EIRVAH_METRIC_PREFIX!r}; "
            "let the factory add the prefix"
        )
    return f"{EIRVAH_METRIC_PREFIX}_{name}"


def make_counter(
    name: str,
    documentation: str,
    *,
    labelnames: Sequence[str],
    registry: CollectorRegistry | None = None,
) -> Counter:
    return Counter(_full_name(name), documentation, labelnames=labelnames, registry=registry)


def make_gauge(
    name: str,
    documentation: str,
    *,
    labelnames: Sequence[str],
    registry: CollectorRegistry | None = None,
) -> Gauge:
    return Gauge(_full_name(name), documentation, labelnames=labelnames, registry=registry)


def make_histogram(
    name: str,
    documentation: str,
    *,
    labelnames: Sequence[str],
    buckets: Sequence[float] = DEFAULT_LATENCY_BUCKETS,
    registry: CollectorRegistry | None = None,
) -> Histogram:
    return Histogram(
        _full_name(name),
        documentation,
        labelnames=labelnames,
        buckets=buckets,
        registry=registry,
    )
