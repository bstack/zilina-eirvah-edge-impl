"""Prometheus metrics for actuation-control-orchestrator."""

from __future__ import annotations

from eirvah_observability.metrics import make_counter, make_histogram
from prometheus_client.registry import REGISTRY, CollectorRegistry


class ActuationMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._approved = make_counter(
            "actuation_approved_total",
            "Actuation requests approved and written",
            labelnames=["path"],
            registry=registry,
        )
        self._rejected = make_counter(
            "actuation_rejected_total",
            "Actuation requests rejected",
            labelnames=["path", "reason"],
            registry=registry,
        )
        self._stage_timeout = make_counter(
            "actuation_stage_timeout_total",
            "Actuation pipeline stage timeouts",
            labelnames=["path", "stage"],
            registry=registry,
        )
        self._e2e_latency = make_histogram(
            "actuation_e2e_latency_seconds",
            "End-to-end latency from NATS ingress to AMQP result",
            labelnames=["path"],
            registry=registry,
        )

    def inc_approved(self, *, path: str) -> None:
        self._approved.labels(path=path).inc()

    def inc_rejected(self, *, path: str, reason: str) -> None:
        self._rejected.labels(path=path, reason=reason).inc()

    def inc_stage_timeout(self, *, path: str, stage: str) -> None:
        self._stage_timeout.labels(path=path, stage=stage).inc()

    def observe_e2e_latency(self, *, path: str, seconds: float) -> None:
        self._e2e_latency.labels(path=path).observe(seconds)
