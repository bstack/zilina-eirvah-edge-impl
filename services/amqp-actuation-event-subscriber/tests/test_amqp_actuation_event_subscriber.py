"""Unit tests for amqp-actuation-event-subscriber."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.ulid import generate_correlation_id


def _sample_request() -> ActuationRequest:
    now = datetime.now(UTC)
    return ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester="decision-agent-stub",
        target_uns_topic=(
            "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
        ),
        requested_value=22.0,
        value_type="double",
        reason="test",
        requested_at=now,
    )


def test_build_nats_envelope_from_actuation_request() -> None:
    from amqp_actuation_event_subscriber.service import build_nats_envelope

    req = _sample_request()
    body = req.model_dump_json().encode()
    envelope = build_nats_envelope(body)

    assert envelope.correlation_id == req.correlation_id
    assert envelope.status == "ok"
    parsed_req = ActuationRequest.model_validate(envelope.payload)
    assert parsed_req.target_uns_topic == req.target_uns_topic


def test_build_nats_envelope_invalid_json_raises() -> None:
    from amqp_actuation_event_subscriber.service import build_nats_envelope

    with pytest.raises(Exception):
        build_nats_envelope(b"not-json")


def test_build_nats_envelope_missing_field_raises() -> None:
    from amqp_actuation_event_subscriber.service import build_nats_envelope

    incomplete = json.dumps({"schema_version": "1.0"}).encode()
    with pytest.raises(Exception):
        build_nats_envelope(incomplete)
