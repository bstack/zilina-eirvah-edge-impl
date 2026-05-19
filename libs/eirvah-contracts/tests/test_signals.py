from datetime import UTC, datetime

import pytest
from eirvah_contracts.signals import (
    NormalizedSignalEnvelope,
    Quality,
    RawSignalEnvelope,
    SignalValueType,
)
from pydantic import ValidationError


def _ts() -> datetime:
    return datetime(2026, 5, 16, 13, 45, 22, 123456, tzinfo=UTC)


def test_raw_signal_round_trips_through_json() -> None:
    raw = RawSignalEnvelope(
        source_endpoint="opc.tcp://opcua-simulator:4840",
        node_id="ns=2;s=Bottler.Temperature01",
        value=23.4,
        value_type="double",
        quality="good",
        source_timestamp=_ts(),
        server_timestamp=_ts(),
        received_at=_ts(),
    )
    parsed = RawSignalEnvelope.model_validate_json(raw.model_dump_json())
    assert parsed == raw


def test_normalized_signal_round_trips_through_json() -> None:
    normed = NormalizedSignalEnvelope(
        node_id="ns=2;s=Bottler.Temperature01",
        value=23.4,
        value_type="double",
        unit="degC",
        quality="good",
        source_timestamp=_ts(),
        received_at=_ts(),
    )
    parsed = NormalizedSignalEnvelope.model_validate_json(normed.model_dump_json())
    assert parsed == normed


@pytest.mark.parametrize("vt", ["double", "int64", "bool", "string"])
def test_value_type_accepts_v1_supported(vt: SignalValueType) -> None:
    RawSignalEnvelope(
        source_endpoint="opc.tcp://x",
        node_id="n",
        value=0,
        value_type=vt,
        quality="good",
        source_timestamp=_ts(),
        server_timestamp=_ts(),
        received_at=_ts(),
    )


def test_value_type_rejects_unsupported() -> None:
    with pytest.raises(ValidationError):
        RawSignalEnvelope(
            source_endpoint="opc.tcp://x",
            node_id="n",
            value=[1, 2, 3],
            value_type="array",  # type: ignore[arg-type]
            quality="good",
            source_timestamp=_ts(),
            server_timestamp=_ts(),
            received_at=_ts(),
        )


@pytest.mark.parametrize("q", ["good", "uncertain", "bad"])
def test_quality_codes(q: Quality) -> None:
    RawSignalEnvelope(
        source_endpoint="opc.tcp://x",
        node_id="n",
        value=0,
        value_type="double",
        quality=q,
        source_timestamp=_ts(),
        server_timestamp=_ts(),
        received_at=_ts(),
    )
