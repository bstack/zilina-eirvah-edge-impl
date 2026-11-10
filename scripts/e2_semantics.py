"""Selected E2 quality/error policy cases; timestamps and malformed inputs are injected."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

from asyncua import ua
from data_converter.service import handle_convert_request, load_rules
from decision_agent_stub.service import DecisionAgentRuntime
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.pipeline import ContextualizeResult
from eirvah_contracts.signals import NormalizedSignalEnvelope
from eirvah_contracts.ulid import generate_correlation_id
from interoperability import (
    AT,
    PROFILE,
    ROOT,
    SOURCES,
    command_meaning,
    digest,
    load_profile,
    native_raw,
    settings,
)
from mqtt_uns_publisher.service import build_sosa_observation
from opcua_data_subscriber.service import build_raw_envelope as opcua_raw
from prometheus_client import CollectorRegistry
from s7_data_subscriber.service import decode_real
from uns_auto_contextualizer.service import handle_contextualize_request, load_ontology
from uns_contextualizer_orchestrator.models import PipelineContext

QUALITY_SOURCES = [*SOURCES, Path(__file__)]
BASE_CASES = (
    "good",
    "stale",
    "future",
    "wrong_unit",
    "wrong_type",
    "wrong_version",
    "missing_version",
    "wrong_identity",
    "wrong_topic",
    "unknown_alias",
)

CAPABILITIES = dict(
    opcua=dict(source_quality="status_code", source_timestamp="server_supplied_or_fallback"),
    modbus=dict(source_quality="not_carried", source_timestamp="adapter_acquisition"),
    s7=dict(source_quality="not_carried", source_timestamp="adapter_acquisition"),
)


def capture_quality() -> dict[str, Any]:
    profile = load_profile(PROFILE)
    sample = next(s for s in profile["samples"] if s["id"] == "speed-high")
    rules = load_rules(ROOT / "deploy/k3s/base/data-converter/conversion-rules.yaml")
    graph = load_ontology(ROOT / "config/eirvah-line-a.jsonld")
    rows = []
    cases = [(p, c) for p in profile["protocols"] for c in BASE_CASES] + [
        ("opcua", "bad"),
        ("opcua", "uncertain"),
        ("s7", "short_packet"),
    ]
    for protocol, case in cases:
        row: dict[str, Any] = dict(
            case=protocol + "/" + case,
            command=None,
            error_kind=None,
            state_reason=None,
            stage="native_decode",
        )
        try:
            if case == "short_packet":
                decode_real(bytes.fromhex("3f"), 0)
                raise RuntimeError("truncated native value was accepted")
            raw = native_raw(protocol, sample, profile["source_bindings"][protocol]["speed"], AT)
            if protocol == "opcua" and case in ("bad", "uncertain"):
                code = (
                    ua.StatusCodes.BadSensorFailure
                    if case == "bad"
                    else ua.StatusCodes.UncertainSensorNotAccurate
                )
                dv = ua.DataValue(
                    ua.Variant(0.7, ua.VariantType.Double),
                    StatusCode_=ua.StatusCode(code),
                    SourceTimestamp=AT,
                    ServerTimestamp=AT,
                )
                raw = opcua_raw(
                    alias=raw.node_id, value=0.7, source_endpoint=raw.source_endpoint, data_value=dv
                )
            if case == "unknown_alias":
                raw = raw.model_copy(update=dict(node_id="Unregistered.Gateway.Speed"))
            row["raw_quality"] = raw.quality
            row["raw"] = raw.model_dump(mode="json")
            row["stage"] = "conversion"
            cid = generate_correlation_id()
            converted = handle_convert_request(
                NATSEnvelope(correlation_id=cid, payload=raw.model_dump(mode="json")), rules
            )
            if converted.status != "ok":
                row["error_kind"] = converted.error.kind if converted.error else "missing_error"
            else:
                normalized = NormalizedSignalEnvelope.model_validate(converted.payload)
                contextual = handle_contextualize_request(
                    NATSEnvelope(correlation_id=cid, payload=normalized.model_dump(mode="json")),
                    graph,
                )
                if contextual.status != "ok":
                    raise RuntimeError("base contextualization failed")
                context = PipelineContext(
                    cid,
                    raw,
                    raw.received_at,
                    normalized,
                    ContextualizeResult.model_validate(contextual.payload),
                )
                packet = build_sosa_observation(context.build_publish_request()).to_jsonld()
                topic = profile["signals"]["speed"]["topic"]
                if case == "stale":
                    packet["sosa:resultTime"] = (AT - timedelta(seconds=6)).isoformat()
                elif case == "future":
                    packet["sosa:resultTime"] = (AT + timedelta(seconds=1)).isoformat()
                elif case == "wrong_unit":
                    packet["eirvah:unit"] = "rpm"
                elif case == "wrong_type":
                    packet["sosa:hasSimpleResult"] = True
                elif case == "wrong_version":
                    packet["eirvah:schemaVersion"] = "2.0"
                elif case == "missing_version":
                    del packet["eirvah:schemaVersion"]
                elif case == "wrong_identity":
                    packet["sosa:madeBySensor"] = {"@id": "urn:wrong"}
                elif case == "wrong_topic":
                    topic = topic.replace("conveyor", "other")
                row.update(stage="public_receiver", packet=packet, topic=topic)
                receiver = DecisionAgentRuntime(settings(profile), registry=CollectorRegistry())
                request = receiver.observation(topic, packet, AT + timedelta(milliseconds=100))
                row["command"] = command_meaning(request)
                row["state_reason"] = receiver.state_decision.get("reason")
        except (ValueError, KeyError) as exc:
            row["error_kind"] = type(exc).__name__
        except Exception as exc:
            if protocol == "s7" and case == "short_packet":
                row["error_kind"] = type(exc).__name__
            else:
                raise
        rows.append(row)
    return dict(
        mode="in_process_policy_checks",
        source_hashes={p.relative_to(ROOT).as_posix(): digest(p) for p in QUALITY_SOURCES},
        rows=rows,
        capabilities=CAPABILITIES,
    )


def evaluate_quality(data: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = dict(
        quality_case="unknown", mode="in_process_policy_checks", checks=[]
    )
    try:
        if data["mode"] != "in_process_policy_checks" or data["source_hashes"] != {
            p.relative_to(ROOT).as_posix(): digest(p) for p in QUALITY_SOURCES
        }:
            return result
        expected = {p + "/" + c for p in ("opcua", "modbus", "s7") for c in BASE_CASES} | {
            "opcua/bad",
            "opcua/uncertain",
            "s7/short_packet",
        }
        rows = data["rows"]
        ids = [r["case"] for r in rows]
        checks = [
            dict(
                check="protocol_capabilities",
                status="pass" if data["capabilities"] == CAPABILITIES else "fail",
            ),
            dict(
                check="complete_matrix",
                status="pass" if len(ids) == len(expected) and set(ids) == expected else "fail",
            ),
        ]
        for row in rows:
            case = row["case"].split("/")[1]
            no_command = row["command"] is None
            if case == "good":
                good = (
                    row["error_kind"] is None
                    and row["command"]["requested_value"] == 26.0
                    and row["state_reason"] == "accepted"
                )
            elif case in ("stale", "future"):
                good = no_command and row["error_kind"] is None and row["state_reason"] == case
            elif case in ("bad", "uncertain"):
                good = (
                    no_command
                    and row["raw_quality"] == case
                    and row["packet"]["eirvah:quality"] == case
                    and row["state_reason"] == "bad_quality"
                )
            elif case == "unknown_alias":
                good = (
                    no_command
                    and row["stage"] == "conversion"
                    and row["error_kind"] == "UnknownNode"
                )
            elif case == "short_packet":
                good = (
                    no_command and row["stage"] == "native_decode" and row["error_kind"] == "error"
                )
            else:
                good = (
                    no_command
                    and row["stage"] == "public_receiver"
                    and row["error_kind"] in ("ValueError", "KeyError", "ValidationError")
                )
            checks.append(dict(check=row["case"], status="pass" if good else "fail"))
        result.update(
            checks=checks,
            quality_case="pass" if all(c["status"] == "pass" for c in checks) else "fail",
        )
    except (KeyError, TypeError, ValueError, OSError):
        pass
    return result
