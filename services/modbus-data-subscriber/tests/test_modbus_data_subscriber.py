from __future__ import annotations

from datetime import UTC, datetime

import pytest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.signals import RawSignalEnvelope
from eirvah_contracts.ulid import is_valid_correlation_id


def test_build_raw_envelope_temperature() -> None:
    from modbus_data_subscriber.service import build_raw_envelope

    now = datetime.now(UTC)
    env = build_raw_envelope(
        alias="Bottler.Temperature01",
        value=22.0,
        value_type="double",
        source_endpoint="modbus-tcp://modbus-simulator:502",
        received_at=now,
    )
    assert isinstance(env, RawSignalEnvelope)
    assert env.node_id == "Bottler.Temperature01"
    assert env.value == pytest.approx(22.0)
    assert env.quality == "good"
    assert env.source_endpoint == "modbus-tcp://modbus-simulator:502"
    assert env.source_timestamp == now
    assert env.server_timestamp == now
    assert env.received_at == now


def test_build_raw_envelope_motor_state_int() -> None:
    from modbus_data_subscriber.service import build_raw_envelope

    now = datetime.now(UTC)
    env = build_raw_envelope(
        alias="Bottler.Motor01.State",
        value=1,
        value_type="int64",
        source_endpoint="modbus-tcp://modbus-simulator:502",
        received_at=now,
    )
    assert env.value == 1
    assert env.value_type == "int64"


def test_wrap_in_nats_envelope() -> None:
    from modbus_data_subscriber.service import build_raw_envelope, wrap_in_nats_envelope

    now = datetime.now(UTC)
    raw = build_raw_envelope(
        alias="Bottler.Temperature01",
        value=22.0,
        value_type="double",
        source_endpoint="modbus-tcp://modbus-simulator:502",
        received_at=now,
    )
    env = wrap_in_nats_envelope(raw)
    assert isinstance(env, NATSEnvelope)
    assert is_valid_correlation_id(env.correlation_id)
    assert env.status == "ok"
    assert env.payload["node_id"] == "Bottler.Temperature01"


def test_apply_scale() -> None:
    from modbus_data_subscriber.service import apply_scale

    assert apply_scale(2200, scale=100.0, value_type="double") == pytest.approx(22.0)
    assert apply_scale(1, scale=1.0, value_type="int64") == 1


def test_load_register_map(tmp_path) -> None:
    from modbus_data_subscriber.service import RegisterMapConfig, load_register_map

    cfg_file = tmp_path / "map.yaml"
    cfg_file.write_text("""
host: modbus-simulator
port: 502
unit_id: 1
poll_interval_ms: 500
registers:
  - address: 0
    alias: Bottler.Temperature01
    scale: 100.0
    value_type: double
  - address: 2
    alias: Bottler.Motor01.State
    scale: 1.0
    value_type: int64
""")
    cfg = load_register_map(cfg_file)
    assert isinstance(cfg, RegisterMapConfig)
    assert cfg.host == "modbus-simulator"
    assert cfg.port == 502
    assert len(cfg.registers) == 2
    assert cfg.registers[0].alias == "Bottler.Temperature01"
    assert cfg.registers[0].scale == 100.0
