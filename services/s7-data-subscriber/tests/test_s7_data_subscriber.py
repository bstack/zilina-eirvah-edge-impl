from __future__ import annotations

import struct
from datetime import UTC, datetime

import pytest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.signals import RawSignalEnvelope
from eirvah_contracts.ulid import is_valid_correlation_id


def test_decode_real() -> None:
    from s7_data_subscriber.service import decode_real
    buf = bytearray(4)
    struct.pack_into(">f", buf, 0, 2.5)
    assert decode_real(buf, 0) == pytest.approx(2.5, abs=0.001)


def test_decode_int() -> None:
    from s7_data_subscriber.service import decode_int
    buf = bytearray(2)
    struct.pack_into(">h", buf, 0, 42)
    assert decode_int(buf, 0) == 42


def test_decode_int_negative() -> None:
    from s7_data_subscriber.service import decode_int
    buf = bytearray(2)
    struct.pack_into(">h", buf, 0, -1)
    assert decode_int(buf, 0) == -1


def test_build_raw_envelope() -> None:
    from s7_data_subscriber.service import build_raw_envelope
    now = datetime.now(UTC)
    env = build_raw_envelope(
        alias="Capper.TorqueSensor01",
        value=2.5,
        value_type="double",
        source_endpoint="s7://s7-simulator:102",
        received_at=now,
    )
    assert isinstance(env, RawSignalEnvelope)
    assert env.node_id == "Capper.TorqueSensor01"
    assert env.value == pytest.approx(2.5)
    assert env.quality == "good"
    assert env.source_endpoint == "s7://s7-simulator:102"
    assert env.source_timestamp == now


def test_wrap_in_nats_envelope() -> None:
    from s7_data_subscriber.service import build_raw_envelope, wrap_in_nats_envelope
    now = datetime.now(UTC)
    raw = build_raw_envelope(
        alias="Capper.TorqueSensor01",
        value=2.5,
        value_type="double",
        source_endpoint="s7://s7-simulator:102",
        received_at=now,
    )
    env = wrap_in_nats_envelope(raw)
    assert isinstance(env, NATSEnvelope)
    assert is_valid_correlation_id(env.correlation_id)
    assert env.payload["node_id"] == "Capper.TorqueSensor01"


def test_load_s7_unit_map(tmp_path) -> None:
    from s7_data_subscriber.service import S7UnitMapConfig, load_s7_unit_map
    cfg_file = tmp_path / "map.yaml"
    cfg_file.write_text("""
host: s7-simulator
port: 102
rack: 0
slot: 1
poll_interval_ms: 1000
data_blocks:
  - db: 1
    size: 8
    signals:
      - offset: 0
        type: real
        alias: Capper.TorqueSensor01
        value_type: double
      - offset: 4
        type: int
        alias: Capper.CapSensor01
        value_type: int64
""")
    cfg = load_s7_unit_map(cfg_file)
    assert isinstance(cfg, S7UnitMapConfig)
    assert cfg.host == "s7-simulator"
    assert len(cfg.data_blocks) == 1
    assert len(cfg.data_blocks[0].signals) == 2
    assert cfg.data_blocks[0].signals[0].alias == "Capper.TorqueSensor01"
