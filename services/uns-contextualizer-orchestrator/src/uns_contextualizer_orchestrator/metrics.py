"""Prometheus metrics for the UNS contextualizer orchestrator (spec §§6.1, 7.2)."""

from __future__ import annotations

from prometheus_client.registry import REGISTRY, CollectorRegistry

from eirvah_observability.metrics import make_counter, make_histogram


class PipelineMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._success = make_counter(
            "pipeline_success_total",
            "Pipeline messages processed successfully end-to-end",
            labelnames=["path"],
            registry=registry,
        )
        self._stage_error = make_counter(
            "pipeline_stage_error_total",
            "Pipeline stage errors (worker replied status=error)",
            labelnames=["path", "stage", "reason"],
            registry=registry,
        )
        self._stage_timeout = make_counter(
            "pipeline_stage_timeout_total",
            "Pipeline stage timeouts",
            labelnames=["path", "stage"],
            registry=registry,
        )
        self._e2e_latency = make_histogram(
            "pipeline_e2e_latency_seconds",
            "End-to-end latency from ingress to MQTT publish",
            labelnames=["path"],
            registry=registry,
        )

    def inc_success(self, *, path: str) -> None:
        self._success.labels(path=path).inc()

    def inc_stage_error(self, *, path: str, stage: str, reason: str) -> None:
        self._stage_error.labels(path=path, stage=stage, reason=reason).inc()

    def inc_stage_timeout(self, *, path: str, stage: str) -> None:
        self._stage_timeout.labels(path=path, stage=stage).inc()

    def observe_e2e_latency(self, *, path: str, seconds: float) -> None:
        self._e2e_latency.labels(path=path).observe(seconds)
