from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.signals import RawSignalEnvelope
from eirvah_contracts.ulid import is_valid_correlation_id


def _make_data_value(value: Any, status_good: bool = True) -> MagicMock:
    dv = MagicMock()
    dv.SourceTimestamp = datetime(2026, 5, 18, 12, 0, 0, tzinfo=UTC)
    dv.ServerTimestamp = datetime(2026, 5, 18, 12, 0, 0, tzinfo=UTC)
    sc = MagicMock()
    sc.is_bad.return_value = not status_good
    sc.is_uncertain.return_value = False
    dv.StatusCode = sc
    return dv


def test_build_raw_envelope_good_quality() -> None:
    from opcua_data_subscriber.service import build_raw_envelope

    dv = _make_data_value(23.4)
    envelope = build_raw_envelope(
        alias="Bottler.Temperature01",
        value=23.4,
        source_endpoint="opc.tcp://opcua-simulator:4840",
        data_value=dv,
    )
    assert isinstance(envelope, RawSignalEnvelope)
    assert envelope.node_id == "Bottler.Temperature01"
    assert envelope.value == 23.4
    assert envelope.quality == "good"
    assert envelope.source_endpoint == "opc.tcp://opcua-simulator:4840"


def test_build_raw_envelope_bad_quality() -> None:
    from opcua_data_subscriber.service import build_raw_envelope

    dv = _make_data_value(99.0, status_good=False)
    envelope = build_raw_envelope(
        alias="Bottler.Temperature01",
        value=99.0,
        source_endpoint="opc.tcp://opcua-simulator:4840",
        data_value=dv,
    )
    assert envelope.quality == "bad"


def test_wrap_in_nats_envelope() -> None:
    from opcua_data_subscriber.service import wrap_in_nats_envelope

    now = datetime.now(UTC)
    raw = RawSignalEnvelope(
        source_endpoint="opc.tcp://opcua-simulator:4840",
        node_id="Bottler.Temperature01",
        value=23.4,
        value_type="double",
        quality="good",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
    )
    env = wrap_in_nats_envelope(raw)
    assert isinstance(env, NATSEnvelope)
    assert is_valid_correlation_id(env.correlation_id)
    assert env.status == "ok"
    assert env.payload is not None
    assert env.payload["node_id"] == "Bottler.Temperature01"


def test_detect_value_type() -> None:
    from opcua_data_subscriber.service import detect_value_type

    assert detect_value_type(True) == "bool"
    assert detect_value_type(42) == "int64"
    assert detect_value_type(3.14) == "double"
    assert detect_value_type("hello") == "string"
