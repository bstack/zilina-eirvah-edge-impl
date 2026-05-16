import pytest
from eirvah_observability.metrics import (
    EIRVAH_METRIC_PREFIX,
    make_counter,
    make_gauge,
    make_histogram,
)
from prometheus_client import CollectorRegistry


def test_counter_carries_eirvah_prefix() -> None:
    reg = CollectorRegistry()
    c = make_counter("test_counter", "doc", labelnames=["stage"], registry=reg)
    c.labels(stage="convert").inc()
    assert (
        reg.get_sample_value(
            f"{EIRVAH_METRIC_PREFIX}_test_counter_total", {"stage": "convert"}
        )
        == 1.0
    )


def test_gauge_carries_eirvah_prefix() -> None:
    reg = CollectorRegistry()
    g = make_gauge("test_gauge_celsius", "doc", labelnames=[], registry=reg)
    g.set(23.4)
    assert (
        reg.get_sample_value(f"{EIRVAH_METRIC_PREFIX}_test_gauge_celsius") == 23.4
    )


def test_histogram_default_buckets_present() -> None:
    reg = CollectorRegistry()
    h = make_histogram("test_latency_seconds", "doc", labelnames=[], registry=reg)
    h.observe(0.5)
    assert (
        reg.get_sample_value(f"{EIRVAH_METRIC_PREFIX}_test_latency_seconds_count")
        == 1.0
    )


def test_make_counter_rejects_name_with_prefix_duplication() -> None:
    reg = CollectorRegistry()
    with pytest.raises(ValueError):
        make_counter("eirvah_double_prefix", "doc", labelnames=[], registry=reg)
