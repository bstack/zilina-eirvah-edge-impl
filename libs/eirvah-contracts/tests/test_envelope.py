import json

import pytest
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.ulid import generate_correlation_id


def test_envelope_round_trips_through_json() -> None:
    env = NATSEnvelope(
        correlation_id=generate_correlation_id(),
        payload={"value": 42},
        context={"stage": "convert"},
    )
    raw = env.model_dump_json()
    parsed = NATSEnvelope.model_validate_json(raw)
    assert parsed == env


def test_envelope_defaults_status_to_ok() -> None:
    env = NATSEnvelope(correlation_id=generate_correlation_id(), payload={"x": 1})
    assert env.status == "ok"
    assert env.error is None


def test_envelope_error_status() -> None:
    env = NATSEnvelope(
        correlation_id=generate_correlation_id(),
        status="error",
        error=EnvelopeError(kind="ValidationError", message="bad input"),
    )
    raw = env.model_dump_json()
    parsed = NATSEnvelope.model_validate_json(raw)
    assert parsed.status == "error"
    assert parsed.error is not None
    assert parsed.error.kind == "ValidationError"


def test_envelope_rejects_invalid_correlation_id() -> None:
    with pytest.raises(ValueError):
        NATSEnvelope(correlation_id="not-a-ulid", payload={})


def test_envelope_rejects_invalid_status() -> None:
    with pytest.raises(ValueError):
        NATSEnvelope(
            correlation_id=generate_correlation_id(),
            payload={},
            status="weird",  # type: ignore[arg-type]
        )


def test_envelope_serialises_with_compact_json() -> None:
    env = NATSEnvelope(correlation_id=generate_correlation_id(), payload={"v": 1})
    raw = env.model_dump_json()
    # No trailing whitespace, parseable
    assert json.loads(raw)["status"] == "ok"
