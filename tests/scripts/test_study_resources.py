"""Resource samples preserve missing coverage and CPU/memory units."""

import os

import pytest
from study_resources import phase_for_window, process_usage, quantity


@pytest.mark.parametrize(
    "value,kind,expected",
    [
        ("250m", "cpu", 0.25),
        ("125000000n", "cpu", 0.125),
        ("128Mi", "memory", 134217728),
        ("2G", "memory", 2000000000),
    ],
)
def test_kubernetes_quantities_are_normalized(value, kind, expected):
    assert quantity(value, kind) == expected


@pytest.mark.parametrize("value", ["NaN", "-1", "3garbage"])
def test_invalid_resource_value_is_not_reported_as_zero(value):
    with pytest.raises(ValueError):
        quantity(value, "cpu")


def test_process_capture_samples_the_explicit_owned_process():
    sample = process_usage(os.getpid())
    assert sample["cpu_seconds"] >= 0
    assert sample["rss_bytes"] > 0
    assert sample["identity"]


def test_phase_attribution_needs_whole_measurement_window_and_clock_bound():
    phases = [dict(name="work", begin=10.0, end=40.0)]
    assert phase_for_window(20, 30, phases, 1) == "work"
    assert phase_for_window(5, 30, phases, 1) is None
    assert phase_for_window(20, 30, phases, None) is None
