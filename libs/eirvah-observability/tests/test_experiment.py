"""Experiment phases are attributable, bounded records rather than inferred log text."""

import json

import pytest
from eirvah_observability.experiment import emit, phase


def test_phase_retains_begin_end_even_when_body_fails(tmp_path, monkeypatch):
    path = tmp_path / "events.jsonl"
    monkeypatch.setenv("EIRVAH_EXPERIMENT_EVENTS", str(path))

    with pytest.raises(ValueError), phase("selected-work"):
        raise ValueError("selected failure")

    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [r["event"] for r in rows] == ["phase_begin", "phase_end"]
    assert rows[0]["phase_id"] == rows[1]["phase_id"]
    assert rows[1]["success"] is False
    assert rows[1]["monotonic_ns"] >= rows[0]["monotonic_ns"]


def test_disabled_capture_does_not_create_an_artifact(monkeypatch):
    monkeypatch.delenv("EIRVAH_EXPERIMENT_EVENTS", raising=False)
    with phase("work"):
        emit("process_started", name="worker", pid=123)
