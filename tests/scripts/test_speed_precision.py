"""Declared signal precision must not hide a genuine range violation."""

import struct
from datetime import UTC, datetime

import pytest
from data_converter.service import ConversionRule, apply_conversion, load_rules
from eirvah_contracts.measurement import EngineeringPrecision, normalize_measurement
from eirvah_contracts.signals import RawSignalEnvelope
from interoperability import PROFILE, ROOT, load_profile

POLICY = dict(resolution=0.01, minimum=0.0, maximum=0.8, encoding_error=3e-8)


@pytest.mark.parametrize(
    "value,expected",
    [
        (0.4, 0.4),
        (0.555, 0.56),
        (0.565, 0.56),
        (0.8, 0.8),
        (struct.unpack(">f", bytes.fromhex("3f4ccccd"))[0], 0.8),
    ],
)
def test_declared_precision_normalizes_native_values(value, expected):
    policy = EngineeringPrecision(**POLICY)

    result = normalize_measurement(value, policy)

    assert result == expected


@pytest.mark.parametrize(
    "value", [0.800001, 0.801, 0.804, 0.81, -0.000001, float("nan"), float("inf"), True, "0.8"]
)
def test_range_and_type_checks_happen_before_rounding(value):
    policy = EngineeringPrecision(**POLICY)

    with pytest.raises(ValueError):
        normalize_measurement(value, policy)


@pytest.mark.parametrize(
    "patch",
    [dict(resolution=0), dict(minimum=1), dict(encoding_error=0.005), dict(maximum=float("inf"))],
)
def test_invalid_precision_contract_is_rejected(patch):
    with pytest.raises(ValueError):
        EngineeringPrecision(**(POLICY | patch))


def test_converter_preserves_original_and_normalizes_after_unit_conversion():
    now = datetime(2026, 11, 7, tzinfo=UTC)
    raw = RawSignalEnvelope(
        source_endpoint="fixture",
        node_id="speed",
        value=80.0,
        value_type="double",
        quality="good",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
    )
    rule = ConversionRule(
        node_id="speed",
        value_type="double",
        unit="m/s",
        scale=0.01,
        precision=EngineeringPrecision(**POLICY),
    )

    normalized = apply_conversion(raw, rule)

    assert raw.value == 80.0
    assert normalized.value == 0.8


def test_selected_configurations_declare_the_same_engineering_policy():
    profile = load_profile(PROFILE)
    for path in [
        ROOT / "config/conversion-rules.yaml",
        ROOT / "deploy/k3s/base/data-converter/conversion-rules.yaml",
    ]:
        policy = load_rules(path)["Conveyor.Belt01.SpeedSetpoint"].precision
        assert policy.model_dump() == POLICY
    for protocol in profile["protocols"]:
        assert profile["source_bindings"][protocol]["speed"]["precision"] == POLICY
