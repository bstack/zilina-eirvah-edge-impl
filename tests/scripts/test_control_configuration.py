from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from control import build_command
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.signals import RawSignalEnvelope

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "target,action,value",
    [
        ("temperature", "set", 25.0),
        ("conveyor", "start", None),
        ("conveyor", "stop", None),
        ("conveyor", "speed", 0.6),
        ("conveyor", "reset", None),
        ("capper", "start", None),
        ("capper", "stop", None),
        ("capper", "speed", 1200),
        ("capper", "reset", None),
        ("filler", "open", None),
        ("filler", "close", None),
        ("reject", "reject", 42),
    ],
)
def test_manual_commands_match_deployed_policy(target: str, action: str, value: object) -> None:
    from actuation_event_validator.service import load_policy, validate_request

    request = build_command(target, action, value)
    policies = load_policy(ROOT / "deploy/k3s/base/actuation-event-validator/actuation-policy.yaml")
    assert validate_request(request, policies).decision == "approve"


@pytest.mark.parametrize("target,value", [("conveyor", True), ("capper", True), ("capper", 1.5)])
def test_wrong_numeric_types_are_rejected(target: str, value: object) -> None:
    with pytest.raises(ValueError):
        build_command(target, "speed", value)


def test_native_feedback_survives_conversion_and_contextualization() -> None:
    from actuation_signal_publisher.native import load_native_targets
    from data_converter.service import handle_convert_request, load_rules
    from uns_auto_contextualizer.service import handle_contextualize_request, load_ontology

    rules = load_rules(ROOT / "deploy/k3s/base/data-converter/conversion-rules.yaml")
    graph = load_ontology(ROOT / "deploy/k3s/base/uns-auto-contextualizer/eirvah-line-a.jsonld")
    targets = load_native_targets(ROOT / "config/native-actuation-targets.yaml")
    assert set(targets) == set(
        load_native_targets(
            ROOT / "deploy/k3s/base/actuation-signal-publisher/native-actuation-targets.yaml"
        )
    )
    aliases = [
        "Filler.Valve01.Open",
        "Conveyor.Belt01.Running",
        "Conveyor.Belt01.SpeedSetpoint",
        "RejectStation.LastRejectedBottle",
        "Capper.Motor01.Running",
        "Capper.Motor01.Faulted",
        "Capper.Motor01.SpeedSetpoint",
        "Capper.Motor01.Rpm",
    ]
    now = datetime.now(UTC)
    for alias in aliases:
        raw = RawSignalEnvelope(
            source_endpoint="test",
            node_id=alias,
            value=0.4 if alias == "Conveyor.Belt01.SpeedSetpoint" else 1,
            value_type=rules[alias].value_type,
            quality="good",
            source_timestamp=now,
            server_timestamp=now,
            received_at=now,
        )
        request = NATSEnvelope(correlation_id="0" * 26, payload=raw.model_dump(mode="json"))
        normalized = handle_convert_request(request, rules)
        assert normalized.status == "ok"
        result = handle_contextualize_request(normalized, graph)
        assert result.status == "ok", result.error
        assert result.payload["sensor_uri"]


def test_value_type_mismatch_does_not_pass_validator() -> None:
    from actuation_event_validator.service import load_policy, validate_request

    policies = load_policy(ROOT / "config/actuation-policy.yaml")
    command = build_command("conveyor", "start")
    altered = command.model_copy(update={"requested_value": "true"})
    assert validate_request(altered, policies).decision == "reject"


def test_single_decision_agent_stops_before_replacement_starts() -> None:
    import yaml

    deployment = yaml.safe_load(
        (ROOT / "deploy/k3s/base/decision-agent-stub/deployment.yaml").read_text(encoding="utf-8")
    )
    assert deployment["spec"]["replicas"] == 1
    assert deployment["spec"].get("strategy", {}).get("type") == "Recreate"
    assert not deployment["spec"]["strategy"].get("rollingUpdate")
