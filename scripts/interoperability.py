"""Bounded E2 preparation: native-format golden replay, not a deployed experiment."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import struct
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from asyncua import ua
from data_converter.service import handle_convert_request, load_rules
from decision_agent_stub.config import DecisionAgentSettings
from decision_agent_stub.service import DecisionAgentRuntime, ProductionModeRule
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.measurement import EngineeringPrecision, normalize_measurement
from eirvah_contracts.observation import load_bindings, validate_observation
from eirvah_contracts.pipeline import ContextualizeResult
from eirvah_contracts.signals import NormalizedSignalEnvelope, RawSignalEnvelope
from eirvah_contracts.ulid import generate_correlation_id
from modbus_data_subscriber.service import apply_scale
from modbus_data_subscriber.service import build_raw_envelope as modbus_raw
from mqtt_uns_publisher.service import build_sosa_observation
from opcua_data_subscriber.service import build_raw_envelope as opcua_raw
from prometheus_client import CollectorRegistry
from s7_data_subscriber.service import build_raw_envelope as s7_raw
from s7_data_subscriber.service import decode_int, decode_real
from uns_auto_contextualizer.service import handle_contextualize_request, load_ontology
from uns_contextualizer_orchestrator.models import PipelineContext

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "config/experiments/e2-golden.json"
AT = datetime(2026, 11, 7, 12, tzinfo=UTC)
SOURCES = [
    PROFILE,
    Path(__file__),
    ROOT / "deploy/k3s/base/data-converter/conversion-rules.yaml",
    ROOT / "config/eirvah-line-a.jsonld",
    *[
        ROOT / "services" / service / "src" / service.replace("-", "_") / name
        for service, name in (
            ("opcua-data-subscriber", "service.py"),
            ("modbus-data-subscriber", "service.py"),
            ("s7-data-subscriber", "service.py"),
            ("data-converter", "service.py"),
            ("uns-auto-contextualizer", "service.py"),
            ("mqtt-uns-publisher", "service.py"),
            ("uns-contextualizer-orchestrator", "models.py"),
            ("decision-agent-stub", "service.py"),
            ("decision-agent-stub", "state.py"),
            ("decision-agent-stub", "config.py"),
        )
    ],
    *[
        ROOT / "libs/eirvah-contracts/src/eirvah_contracts" / name
        for name in (
            "signals.py",
            "sosa.py",
            "pipeline.py",
            "observation.py",
            "uns.py",
            "measurement.py",
        )
    ],
]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_profile(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


def settings(profile: dict[str, Any]) -> DecisionAgentSettings:
    return DecisionAgentSettings(
        observation_contract_path=ROOT / "config/eirvah-line-a.jsonld",
        inbox_path=None,
        require_positions=False,
        enabled=True,
        inspection_enabled=False,
        normal_speed_max=0.5,
        high_speed_min=0.6,
        normal_setpoint=22.0,
        high_setpoint=26.0,
        max_observation_age_s=5.0,
        target_uns_topic=profile["command_target"],
        subscribe_topic=profile["signals"]["speed"]["topic"],
        reject_count_topic=profile["signals"]["count"]["topic"],
    )


def native_raw(
    protocol: str, sample: dict[str, Any], binding: dict[str, Any], at: datetime
) -> RawSignalEnvelope:
    endpoint = {
        "opcua": "opc.tcp://fixture.invalid:4840",
        "modbus": "modbus://fixture.invalid:5020",
        "s7": "s7://fixture.invalid:102",
    }[protocol]
    if protocol == "opcua":
        variant = ua.Variant(
            sample["opcua"],
            ua.VariantType.Int64 if binding["value_type"] == "int64" else ua.VariantType.Double,
        )
        data_value = ua.DataValue(variant, SourceTimestamp=at, ServerTimestamp=at)
        return opcua_raw(
            alias=binding["node"],
            value=sample["opcua"],
            source_endpoint=endpoint,
            data_value=data_value,
        )
    if protocol == "modbus":
        value = apply_scale(
            sample["modbus"], scale=binding["scale"], value_type=binding["value_type"]
        )
        return modbus_raw(
            alias=binding["node"],
            value=value,
            value_type=binding["value_type"],
            source_endpoint=endpoint,
            received_at=at,
        )
    payload = bytes.fromhex(sample["s7"])
    value = decode_int(payload, 0) if binding["native_type"] == "int" else decode_real(payload, 0)
    return s7_raw(
        alias=binding["node"],
        value=value,
        value_type=binding["value_type"],
        source_endpoint=endpoint,
        received_at=at,
    )


def source_specific_value(
    protocol: str, sample: dict[str, Any], binding: dict[str, Any]
) -> float | int:
    if protocol == "opcua":
        value: float | int = sample["opcua"]
    elif protocol == "modbus":
        value = (
            sample["modbus"]
            if binding["value_type"] == "int64"
            else sample["modbus"] / binding["scale"]
        )
    else:
        value = struct.unpack(
            ">h" if binding["native_type"] == "int" else ">f", bytes.fromhex(sample["s7"])
        )[0]
    return value


def local_meaning(
    protocol: str, sample: dict[str, Any], binding: dict[str, Any], at: datetime
) -> dict[str, Any]:
    """Baseline owns its mappings; both variants obey the same engineering requirement."""
    value = source_specific_value(protocol, sample, binding)
    if binding.get("precision") is not None:
        value = normalize_measurement(
            value, EngineeringPrecision.model_validate(binding["precision"])
        )
    return {
        **{
            k: binding[k]
            for k in ("node", "topic", "unit", "value_type", "sensor", "feature", "property")
        },
        "value": value,
        "quality": "good",
        "schema_version": "1.0",
        "observed_at": at.isoformat(),
    }


def command_meaning(command: ActuationRequest | None) -> dict[str, Any] | None:
    if command is None:
        return None
    return command.model_dump(mode="json", exclude={"correlation_id", "reason"})


def run_case(profile: dict[str, Any]) -> dict[str, Any]:
    rules = load_rules(ROOT / "deploy/k3s/base/data-converter/conversion-rules.yaml")
    graph = load_ontology(ROOT / "config/eirvah-line-a.jsonld")
    bindings = load_bindings(ROOT / "config/eirvah-line-a.jsonld")
    rows = []
    for protocol in profile["protocols"]:
        for variant in profile["variants"]:
            cfg = settings(profile)
            receiver = DecisionAgentRuntime(cfg, registry=CollectorRegistry())
            baseline = ProductionModeRule(cfg)
            for index, sample in enumerate(profile["samples"]):
                at = AT + timedelta(seconds=index)
                now = at + timedelta(milliseconds=100)
                row: dict[str, Any] = dict(protocol=protocol, variant=variant, sample=sample["id"])
                binding = profile["source_bindings"][protocol][sample["signal"]]
                try:
                    row["native_input"] = sample[protocol]
                    row["source_value"] = source_specific_value(protocol, sample, binding)
                    if variant == "canonical":
                        raw = native_raw(protocol, sample, binding, at)
                        row["raw"] = raw.model_dump(mode="json")
                        cid = generate_correlation_id()
                        converted = handle_convert_request(
                            NATSEnvelope(correlation_id=cid, payload=raw.model_dump(mode="json")),
                            rules,
                        )
                        if converted.status != "ok":
                            raise ValueError("conversion rejected")
                        normalized = NormalizedSignalEnvelope.model_validate(converted.payload)
                        contextual = handle_contextualize_request(
                            NATSEnvelope(
                                correlation_id=cid, payload=normalized.model_dump(mode="json")
                            ),
                            graph,
                        )
                        if contextual.status != "ok":
                            raise ValueError("contextualization rejected")
                        context = PipelineContext(
                            cid,
                            raw,
                            raw.received_at,
                            normalized,
                            ContextualizeResult.model_validate(contextual.payload),
                        )
                        request = context.build_publish_request()
                        packet = build_sosa_observation(request).to_jsonld()
                        observation = validate_observation(request.uns_topic, packet, bindings)
                        meaning = dict(
                            node=raw.node_id,
                            topic=request.uns_topic,
                            unit=observation.unit,
                            value_type=observation.value_type,
                            sensor=observation.made_by_sensor,
                            feature=observation.has_feature_of_interest,
                            property=observation.observed_property,
                            value=observation.has_simple_result,
                            quality=observation.quality,
                            schema_version=observation.schema_version,
                            observed_at=observation.result_time.isoformat(),
                        )
                        command = receiver.observation(request.uns_topic, packet, now)
                        row.update(
                            normalized=normalized.model_dump(mode="json"), public_payload=packet
                        )
                    else:
                        meaning = local_meaning(protocol, sample, binding, at)
                        command = (
                            baseline.update(
                                speed=float(meaning["value"]),
                                observed_at=at,
                                now=now,
                                quality=str(meaning["quality"]),
                            )
                            if sample["signal"] == "speed"
                            else None
                        )
                    row.update(meaning=meaning, command=command_meaning(command))
                except (ValueError, TypeError, KeyError, struct.error) as exc:
                    row["error"] = type(exc).__name__
                rows.append(row)
    return dict(
        schema_version="1.0",
        profile=profile,
        source_hashes={p.relative_to(ROOT).as_posix(): digest(p) for p in SOURCES},
        mode="in_process_native_format_replay",
        native_transport="not_exercised",
        broker_delivery="not_exercised",
        physical_effects="not_exercised",
        rows=rows,
    )


def evaluate(data: dict[str, Any]) -> dict[str, Any]:
    """Compare every result to pinned expectations, not merely to the other variant."""
    checks: list[dict[str, str]] = []
    profile = load_profile(PROFILE)
    attributed = data.get("source_hashes") == {
        p.relative_to(ROOT).as_posix(): digest(p) for p in SOURCES
    }
    scoped = all(
        data.get(k) == value
        for k, value in {
            "schema_version": "1.0",
            "mode": "in_process_native_format_replay",
            "native_transport": "not_exercised",
            "broker_delivery": "not_exercised",
            "physical_effects": "not_exercised",
            "profile": profile,
        }.items()
    )
    checks.append(
        dict(check="attribution_and_scope", status="pass" if attributed and scoped else "unknown")
    )
    rows = data.get("rows", [])
    expected_keys = {
        (p, v, s["id"])
        for p in profile["protocols"]
        for v in profile["variants"]
        for s in profile["samples"]
    }
    keys = [(r.get("protocol"), r.get("variant"), r.get("sample")) for r in rows]
    complete = len(keys) == len(expected_keys) and set(keys) == expected_keys
    checks.append(dict(check="complete_matrix", status="pass" if complete else "fail"))
    samples = {s["id"]: (i, s) for i, s in enumerate(profile["samples"])}
    for row in rows:
        name = "/".join(str(row.get(k)) for k in ("protocol", "variant", "sample"))
        valid = False
        try:
            index, sample = samples[row["sample"]]
            at = AT + timedelta(seconds=index)
            now = at + timedelta(milliseconds=100)
            expected = {
                **profile["signals"][sample["signal"]],
                "value": sample["expected"],
                "quality": "good",
                "schema_version": "1.0",
                "observed_at": at.isoformat(),
            }
            if sample.get("expected_error"):
                valid = (
                    row.get("error") == sample["expected_error"]
                    and "command" not in row
                    and "meaning" not in row
                    and row["native_input"] == sample[row["protocol"]]
                )
                checks.append(dict(check=name, status="pass" if valid else "fail"))
                continue
            actual = row["meaning"]
            value = actual["value"]
            same_value = type(value) is type(expected["value"]) and (
                math.isfinite(value)
                and math.isclose(
                    value, expected["value"], rel_tol=0, abs_tol=profile["absolute_tolerance"]
                )
            )
            expected_command = (
                None
                if sample["expected_setpoint"] is None
                else dict(
                    schema_version="1.0",
                    requester="decision-agent-stub",
                    target_uns_topic=profile["command_target"],
                    requested_value=sample["expected_setpoint"],
                    value_type="double",
                    requested_at=now.isoformat().replace("+00:00", "Z"),
                    deadline=(now + timedelta(seconds=10)).isoformat().replace("+00:00", "Z"),
                )
            )
            wire_ok = True
            if row["variant"] == "canonical":
                packet = row["public_payload"]
                wire_ok = all(
                    (
                        packet["eirvah:unit"] == actual["unit"],
                        packet["eirvah:valueType"] == actual["value_type"],
                        packet["sosa:hasSimpleResult"] == value,
                        packet["sosa:madeBySensor"]["@id"] == actual["sensor"],
                        packet["sosa:hasFeatureOfInterest"]["@id"] == actual["feature"],
                        packet["sosa:observedProperty"]["@id"] == actual["property"],
                        packet["sosa:resultTime"] == actual["observed_at"],
                        packet["eirvah:quality"] == actual["quality"],
                        packet["eirvah:schemaVersion"] == actual["schema_version"],
                        row["normalized"]["value"] == value,
                        row["raw"]["value"] == row["source_value"],
                    )
                )
            valid = (
                wire_ok
                and row["native_input"] == sample[row["protocol"]]
                and "error" not in row
                and same_value
                and (
                    {k: v for k, v in actual.items() if k != "value"}
                    == {k: v for k, v in expected.items() if k != "value"}
                )
                and row["command"] == expected_command
            )
        except (KeyError, TypeError, ValueError):
            valid = False
        checks.append(dict(check=name, status="pass" if valid else "fail"))
    statuses = {c["status"] for c in checks}
    return dict(
        preparation="fail"
        if "fail" in statuses
        else "unknown"
        if "unknown" in statuses
        else "pass",
        C04="unknown",
        study="unknown",
        scope=profile["scope"],
        checks=checks,
        limitations=profile["limitations"],
    )


def precision_probe(profile: dict[str, Any]) -> dict[str, Any]:
    """Compare normalized output with the unchanged rule fed the original decoded value."""
    selected = copy.deepcopy(profile)
    selected["samples"] = [
        dict(
            id="speed-upper-bound",
            signal="speed",
            expected=0.8,
            opcua=0.8,
            modbus=80,
            s7="3f4ccccd",
            expected_setpoint=26.0,
        )
    ]
    data = run_case(selected)
    cases = []
    for row in data["rows"]:
        command = row.get("command")
        legacy = ProductionModeRule(settings(profile)).update(
            speed=row["source_value"], observed_at=AT, now=AT + timedelta(milliseconds=100)
        )
        passed = command is not None and command.get("requested_value") == 26.0
        cases.append(
            dict(
                protocol=row["protocol"],
                variant=row["variant"],
                decoded_value=row.get("source_value"),
                normalized_value=row.get("meaning", {}).get("value"),
                unrounded_command=command_meaning(legacy),
                command=command,
                status="pass" if passed else "fail",
            )
        )
    return dict(
        scope="Synthetic 0.8 m/s probe comparing declared normalization with unrounded rule input",
        status="pass" if all(c["status"] == "pass" for c in cases) else "fail",
        native_transport="not_exercised",
        policy_decision="declared_0.01_m_per_s",
        cases=cases,
        evidence=data,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or ROOT / "results/interoperability" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    output.mkdir(parents=True, exist_ok=False)
    data = run_case(load_profile(PROFILE))
    data["git_sha"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    data["worktree_dirty"] = bool(
        subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()
    )
    report = evaluate(data)
    probe = precision_probe(load_profile(PROFILE))
    report["precision_boundary"] = {k: v for k, v in probe.items() if k != "evidence"}
    if probe["status"] != "pass":
        report["preparation"] = "fail"
    (output / "precision-probe.json").write_text(
        json.dumps(probe, indent=2) + "\n", encoding="utf-8"
    )
    for path in SOURCES:
        destination = output / "sources" / path.relative_to(ROOT)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(path.read_bytes())
    (output / "evidence.json").write_text(
        json.dumps(data, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# E2 interoperability preparation",
        "",
        f"Preparation: **{report['preparation']}**. C04 and study: **unknown**.",
        "",
        report["scope"],
        "",
        "| Check | Verdict |",
        "| --- | --- |",
    ]
    lines += [f"| {c['check']} | {c['status']} |" for c in report["checks"]]
    lines += [
        "",
        "## Precision boundary probe",
        "",
        f"Upper speed bound: **{probe['status']}**. Declared 0.01 m/s resolution applied; original values retained.",
        "",
    ]
    lines += [
        f"- {c['protocol']}/{c['variant']}: {c['status']} (decoded {c['decoded_value']})"
        for c in probe["cases"]
    ]
    lines += ["", "## Limits", ""] + ["- " + s for s in report["limitations"]]
    (output / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        json.dumps(
            dict(
                preparation=report["preparation"], C04="unknown", report=str(output / "report.md")
            ),
            indent=2,
        )
    )
    raise SystemExit(0 if report["preparation"] == "pass" else 1)


if __name__ == "__main__":
    main()
