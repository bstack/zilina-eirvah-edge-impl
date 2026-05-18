from __future__ import annotations

from datetime import UTC, datetime

import pytest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.signals import RawSignalEnvelope


def _raw(node_id: str = "Bottler.Temperature01", value: object = 23.4) -> RawSignalEnvelope:
    now = datetime.now(UTC)
    return RawSignalEnvelope(
        source_endpoint="opc.tcp://test:4840",
        node_id=node_id,
        value=value,
        value_type="double",
        quality="good",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
    )


def test_convert_passthrough() -> None:
    from data_converter.service import ConversionRule, apply_conversion

    raw = _raw()
    rule = ConversionRule(
        node_id="Bottler.Temperature01",
        value_type="double",
        unit="degC",
        drop_bad_quality=False,
    )
    normalized = apply_conversion(raw, rule)
    assert normalized is not None
    assert normalized.node_id == "Bottler.Temperature01"
    assert normalized.value == 23.4
    assert normalized.unit == "degC"
    assert normalized.quality == "good"


def test_convert_with_scale_and_offset() -> None:
    from data_converter.service import ConversionRule, apply_conversion

    raw = _raw(value=100.0)
    rule = ConversionRule(
        node_id="Bottler.Temperature01",
        value_type="double",
        unit="degC",
        drop_bad_quality=False,
        scale=0.1,
        offset=-10.0,
    )
    normalized = apply_conversion(raw, rule)
    assert normalized is not None
    assert abs(float(normalized.value) - 0.0) < 1e-9  # 100 * 0.1 - 10.0 = 0.0


def test_convert_drops_bad_quality_when_configured() -> None:
    from data_converter.service import ConversionRule, apply_conversion

    now = datetime.now(UTC)
    raw = RawSignalEnvelope(
        source_endpoint="opc.tcp://test:4840",
        node_id="Bottler.Temperature01",
        value=23.4,
        value_type="double",
        quality="bad",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
    )
    rule = ConversionRule(
        node_id="Bottler.Temperature01",
        value_type="double",
        unit="degC",
        drop_bad_quality=True,
    )
    result = apply_conversion(raw, rule)
    assert result is None


def test_handle_request_ok() -> None:
    from data_converter.service import ConversionRule, handle_convert_request

    raw = _raw()
    rules = {
        "Bottler.Temperature01": ConversionRule(
            node_id="Bottler.Temperature01",
            value_type="double",
            unit="degC",
            drop_bad_quality=False,
        )
    }
    req_env = NATSEnvelope(
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
        payload=raw.model_dump(mode="json"),
    )
    reply = handle_convert_request(req_env, rules)
    assert reply.status == "ok"
    assert reply.payload is not None
    assert reply.payload["unit"] == "degC"


def test_handle_request_unknown_node() -> None:
    from data_converter.service import handle_convert_request

    raw = _raw(node_id="Unknown.Node")
    rules: dict = {}
    req_env = NATSEnvelope(
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
        payload=raw.model_dump(mode="json"),
    )
    reply = handle_convert_request(req_env, rules)
    assert reply.status == "error"
    assert reply.error is not None
    assert "Unknown.Node" in reply.error.message
